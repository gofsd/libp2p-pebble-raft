//go:build !android

// Package ipc implements local (same-machine) request/response IPC between
// the short-lived mage CLI process and a long-running kvnode daemon, over
// github.com/gofsd/shmring shared-memory ring buffers.
//
// This file is the desktop (linux/darwin/windows) transport, built on
// shmring's name-based CreateShm/OpenShm. GOOS=android inherits Go's
// "linux" build tag (a long-standing special case in the toolchain's
// build-constraint matching), so it needs excluding explicitly here, same
// as shmring's own backend does for the same reason -- see ipc_android.go
// for the real Android transport and why it has to be a different design
// entirely (ASharedMemory, which is what Android actually provides, has no
// name-based rendezvous at all).
//
// # Design
//
// shmring ring buffers are single-producer/single-consumer for their whole
// lifetime: whoever calls CreateShm owns header initialization and, later,
// removal (CloseStorage); the other side OpenShm's the same name as a
// read-only consumer. That is a poor fit for "one long-running daemon, many
// independent short-lived clients" unless every request/response gets a
// fresh pair of segments. So each call to Call:
//
//  1. CreateShm's the node's request channel (client is producer), writes
//     one fixed-size ipcproto.Request carrying a fresh random ID, and
//     Close()s it.
//  2. Waits for a response channel named after that same ID to appear
//     (OpenShm with retry) and reads one fixed-size ipcproto.Response from
//     it.
//  3. Removes the request segment (CloseStorage) now that the response
//     proves the daemon already read it.
//
// Serve mirrors this on the daemon side: OpenShm the (fixed-name) request
// channel (blocking/retrying between commands), read exactly one Request,
// handle it, CreateShm a response channel named after the request's ID and
// write the Response, then loop back to wait for the next request -- once
// it sees one with a different ID than the round it just answered, it
// knows the client has moved on and can safely remove the previous round's
// response segment.
//
// # Why the response channel is named per request, not fixed per node
//
// It used to be a single fixed name, reused every round like the request
// channel. That was a genuine, silent-request-loss bug: the daemon only
// removed the *previous* round's response segment once it had separately
// confirmed (via its own polling, up to openRetryInterval later) that the
// client had torn down the previous round's request segment. A client
// that issued a second Call immediately after the first -- no human typing
// a pause between two `mage` commands, e.g. an automated Set immediately
// followed by a Get, or two nodes bootstrapping back to back -- could
// start polling for its *own* response before that stale segment was
// gone, OpenShm it by the (same, reused) name, read the *previous*
// round's response, and mistake that for proof its own (actually still
// unread) request had been handled. It would then remove its own request
// segment out from under the daemon -- silently dropping the real request
// while reporting false success back to the caller. Naming the response
// channel after a nonce the daemon never reuses makes that impossible by
// construction: a client can never open a segment it didn't itself just
// ask the daemon to create for this exact round.
//
// # Why the request channel is fixed per node, and how replay is avoided
// without a wait
//
// The daemon needs a well-known rendezvous point to discover a request it
// hasn't seen yet, so the request channel name stays fixed per node
// (derived from its peer id) and is re-created fresh by the client on
// every round trip. A shmring.Reader always starts reading from offset 0
// of whatever segment it opens, with no memory of where a previous Reader
// on the same name left off -- so if Serve looped straight back to
// opening the request channel by name before the client had torn down the
// segment from the round it just handled, it would reopen that same
// still-alive segment and read the same bytes again.
//
// An earlier version of this code handled that by blocking until the name
// disappeared before looking for the next request. That introduced its
// own deadlock: the client creates the *next* round's request segment
// (same fixed name) as soon as its previous Call returns, which can race
// ahead of the daemon's own polling confirmation that the *previous*
// segment is gone; the daemon's wait would then mistake the brand new
// segment for the still-lingering old one and block on it forever,
// since the daemon itself is what would need to read it to make it go
// away. Serve instead just re-reads whatever it finds and compares the
// decoded Request's ID against the last one it actually handled: a match
// means this is the same request it already answered (the client hasn't
// removed it yet), so it waits a beat and rereads rather than
// reprocessing; any other ID is a genuinely new request, safe to handle
// immediately regardless of whether the segment is "new" or one the
// daemon simply hasn't noticed disappear yet.
//
// Together this only supports one in-flight request per node at a time --
// adequate for a single operator driving commands sequentially from a
// CLI.
package ipc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipcproto"
)

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the larger of Request/Response.
const capacity = 4096

const (
	minPoll = 200 * time.Microsecond
	maxPoll = 5 * time.Millisecond

	openRetryInterval = 20 * time.Millisecond
)

func reqChannel(peerID string) string { return "kvipc-" + peerID + "-req" }

// respChannel is unique per round trip (see package doc comment): id is
// the originating Request's ID, which the daemon echoes into its Response.
func respChannel(peerID string, id uint64) string {
	return fmt.Sprintf("kvipc-%s-resp-%d", peerID, id)
}

// newRequestID returns a random nonce for a Request.ID. Collisions are not
// safety-critical (at worst two concurrent callers to the same node would
// need to fall back to their retry loop), just astronomically unlikely at
// 64 bits of entropy for a single operator's sequential CLI usage.
func newRequestID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b[:])
}

// Call sends req to the daemon serving peerID and returns its response. It
// blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, req ipcproto.Request) (ipcproto.Response, error) {
	req.ID = newRequestID()

	rn := reqChannel(peerID)
	w, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		return ipcproto.Response{}, fmt.Errorf("ipc: create request channel: %w", err)
	}

	buf := req.Encode()
	if _, err := w.WriteContext(ctx, buf[:]); err != nil {
		w.CloseStorage()
		return ipcproto.Response{}, fmt.Errorf("ipc: write request: %w", err)
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		return ipcproto.Response{}, fmt.Errorf("ipc: close request writer: %w", err)
	}

	r, err := openRespWithRetry(ctx, peerID, req.ID)
	if err != nil {
		w.CloseStorage()
		return ipcproto.Response{}, err
	}

	respBuf := make([]byte, ipcproto.ResponseSize)
	if err := readFull(ctx, r, respBuf); err != nil {
		r.Close()
		w.CloseStorage()
		return ipcproto.Response{}, fmt.Errorf("ipc: read response: %w", err)
	}
	r.Close()

	// The response proves the daemon already fully read the request; safe
	// to remove the request segment now.
	w.CloseStorage()

	resp, err := ipcproto.DecodeResponse(respBuf)
	if err != nil {
		return ipcproto.Response{}, err
	}
	return resp, nil
}

func openRespWithRetry(ctx context.Context, peerID string, id uint64) (*shmring.Reader, error) {
	name := respChannel(peerID, id)
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("ipc: waiting for response channel %s: %w", name, ctx.Err())
		case <-time.After(openRetryInterval):
		}
	}
}

func readFull(ctx context.Context, r *shmring.Reader, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := r.ReadContext(ctx, buf[total:])
		total += n
		if err != nil {
			return err
		}
	}
	return nil
}

// Handler processes one decoded Request and returns the Response to send
// back.
type Handler func(ctx context.Context, req ipcproto.Request) ipcproto.Response

// Serve runs the daemon side of the protocol for peerID: it repeatedly waits
// for a request, dispatches it to handle, and sends back the response. It
// blocks until ctx is done.
func Serve(ctx context.Context, peerID string, handle Handler) error {
	name := reqChannel(peerID)

	var lastID uint64
	var haveLastID bool
	var pendingResp *shmring.Writer
	cleanupPending := func() {
		if pendingResp != nil {
			pendingResp.CloseStorage()
			pendingResp = nil
		}
	}
	defer cleanupPending()

	for {
		r, err := openReqWithRetry(ctx, name)
		if err != nil {
			return err
		}

		reqBuf := make([]byte, ipcproto.RequestSize)
		err = readFull(ctx, r, reqBuf)
		r.Close()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		req, err := ipcproto.DecodeRequest(reqBuf)
		if err != nil {
			continue
		}

		if haveLastID && req.ID == lastID {
			// The same request segment we already answered, reopened
			// before the client has torn it down -- see the package doc
			// comment on why we reread and dedup by ID instead of
			// blocking for the name to disappear. Give the client a beat
			// to catch up and try again.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(openRetryInterval):
			}
			continue
		}

		// A genuinely new request only appears once the client's previous
		// Call has returned (single in-flight caller), which only happens
		// after it read our previous response -- safe to remove it now.
		cleanupPending()

		resp := handle(ctx, req)
		resp.ID = req.ID
		respWriter, err := sendResponse(ctx, peerID, resp)
		if err != nil {
			return err
		}
		pendingResp = respWriter
		lastID = req.ID
		haveLastID = true
	}
}

func openReqWithRetry(ctx context.Context, name string) (*shmring.Reader, error) {
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(openRetryInterval):
		}
	}
}

// sendResponse creates the response channel (named after resp.ID, which the
// caller must have set to the originating Request's ID), writes resp, and
// closes the writer (marking it done, but not yet removing the segment).
// The caller must CloseStorage the returned writer once it has
// independently confirmed the client has read the response (Serve does
// this lazily, once it sees the next round's distinct request ID).
func sendResponse(ctx context.Context, peerID string, resp ipcproto.Response) (*shmring.Writer, error) {
	w, err := shmring.CreateShm(respChannel(peerID, resp.ID), capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		return nil, fmt.Errorf("ipc: create response channel: %w", err)
	}
	buf := resp.Encode()
	if _, err := w.WriteContext(ctx, buf[:]); err != nil {
		w.CloseStorage()
		return nil, fmt.Errorf("ipc: write response: %w", err)
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		return nil, fmt.Errorf("ipc: close response writer: %w", err)
	}
	return w, nil
}
