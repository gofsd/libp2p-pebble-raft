// Package ipc implements local (same-machine) request/response IPC between
// the short-lived mage CLI process and a long-running kvnode daemon, over
// github.com/gofsd/shmring shared-memory ring buffers.
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
//     one fixed-size ipcproto.Request, and Close()s it.
//  2. Waits for the node's response channel to appear (OpenShm with retry)
//     and reads one fixed-size ipcproto.Response from it.
//  3. Removes the request segment (CloseStorage) now that the response
//     proves the daemon already read it.
//
// Serve mirrors this on the daemon side: OpenShm the request channel
// (blocking/retrying between commands), read exactly one Request, handle
// it, CreateShm the response channel and write the Response, then wait for
// the client to remove the request segment -- which only happens once the
// client has read the response -- before removing the response segment and
// looping back to wait for the next request.
//
// That last wait matters for more than cleanup: a shmring.Reader always
// starts reading from offset 0 of whatever segment it opens, with no
// memory of where a previous Reader on the same name left off. If Serve
// looped straight back to opening the request channel by name, and the
// client hadn't yet torn down the segment from the round it just handled,
// it would reopen that same still-alive segment and silently replay the
// request it already processed. Waiting for the name to disappear first
// rules that out.
//
// Both channel names are fixed per node (derived from its peer id) and are
// re-created fresh on every round trip, so this only supports one
// in-flight request per node at a time -- adequate for a single operator
// driving commands sequentially from a CLI.
package ipc

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-pebble-raft/pkg/ipcproto"
)

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the larger of Request/Response.
const capacity = 4096

const (
	minPoll = 200 * time.Microsecond
	maxPoll = 5 * time.Millisecond

	openRetryInterval = 20 * time.Millisecond
)

func reqChannel(peerID string) string  { return "kvipc-" + peerID + "-req" }
func respChannel(peerID string) string { return "kvipc-" + peerID + "-resp" }

// Call sends req to the daemon serving peerID and returns its response. It
// blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, req ipcproto.Request) (ipcproto.Response, error) {
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

	r, err := openRespWithRetry(ctx, peerID)
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

func openRespWithRetry(ctx context.Context, peerID string) (*shmring.Reader, error) {
	name := respChannel(peerID)
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

		resp := handle(ctx, req)
		respWriter, err := sendResponse(ctx, peerID, resp)
		if err != nil {
			return err
		}

		// A shmring.Reader always starts reading from offset 0 of whatever
		// segment it opens: it does not persist across rounds, and does
		// not know where a *previous* reader on the same name left off.
		// If we looped straight back to openReqWithRetry, and the client
		// hadn't yet removed this round's request segment (it does so
		// only after reading our response), we'd reopen the very same
		// still-alive segment and silently replay the request we just
		// handled. Wait for the client to tear it down before looking for
		// the next one -- which also proves the client has already read
		// our response, so it's safe to remove the response segment too.
		waitErr := waitForAbsence(ctx, name)
		respWriter.CloseStorage()
		if waitErr != nil {
			return waitErr
		}
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

// waitForAbsence blocks until name no longer refers to a live shared-memory
// segment (or ctx is done).
func waitForAbsence(ctx context.Context, name string) error {
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err != nil {
			return nil
		}
		r.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(openRetryInterval):
		}
	}
}

// sendResponse creates the response channel, writes resp, and closes the
// writer (marking it done, but not yet removing the segment). The caller
// must CloseStorage the returned writer once it has independently confirmed
// the client has read the response (Serve does this via waitForAbsence on
// the request channel).
func sendResponse(ctx context.Context, peerID string, resp ipcproto.Response) (*shmring.Writer, error) {
	w, err := shmring.CreateShm(respChannel(peerID), capacity, shmring.WithPollInterval(minPoll, maxPoll))
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
