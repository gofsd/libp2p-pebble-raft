//go:build android

// Package ipc, on Android, implements the same Call/Serve contract as the
// desktop transport (ipc.go) but over shmring's ASharedMemory backend
// instead of name-based CreateShm/OpenShm.
//
// ASharedMemory has no name-based rendezvous at all -- see
// shmring/backend/android.go's doc comment -- so a segment can only be
// shared by handing over its raw file descriptor directly. That only works
// within a single process (or across processes via Binder, which is
// app-specific plumbing this package doesn't attempt -- see
// shmring/mobile's doc comment). Consequently this transport only supports
// a client and a Serve loop running in the same process, which is exactly
// this project's Android build: the follower daemon and the UI's Set/Get
// calls both run inside the one app process (see the mobile package),
// genuinely exercising shmring's shared-memory ring buffer even though no
// OS process boundary is crossed.
//
// Each Call hands its request segment's fd to the matching Serve loop
// through an in-process Go channel (a "mailbox" keyed by peerID, since
// there's still conceptually one daemon per peer id, same as desktop).
// Serve replies by creating a fresh response segment and handing *its* fd
// back the same way. Because every round trip gets its own fresh segments
// -- never reused, unlike desktop's fixed-name request channel -- there is
// no equivalent of the desktop transport's stale-segment race (see ipc.go)
// to guard against here by construction.
package ipc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-pebble-raft/pkg/ipcproto"
)

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the larger of Request/Response.
const capacity = 4096

// call is what Call hands to Serve via the per-peer mailbox.
type call struct {
	reqFD    int
	respChan chan respHandoff
}

// respHandoff carries the response segment's fd back to Call, plus an ack
// Call closes once it has opened (dup'd) that fd. Serve must wait for that
// ack before it may CloseStorage its own writer: an ASharedMemory fd is
// the only thing keeping the region alive until the other side dups it, so
// closing it any earlier would free memory Call hasn't attached to yet
// (shmring.OpenAndroidSharedMemory dups on entry specifically so each side
// ends up with an independent fd/mapping safe to close on its own schedule
// -- but only once that dup has actually happened).
type respHandoff struct {
	fd  int
	ack chan struct{}
}

var (
	mailboxMu sync.Mutex
	mailboxes = map[string]chan call{}
)

// mailboxFor returns the (lazily created) channel Call and Serve rendezvous
// on for peerID. Unbuffered: a Call's send only completes once a Serve
// loop for the same peerID is actively waiting for it.
func mailboxFor(peerID string) chan call {
	mailboxMu.Lock()
	defer mailboxMu.Unlock()
	ch, ok := mailboxes[peerID]
	if !ok {
		ch = make(chan call)
		mailboxes[peerID] = ch
	}
	return ch
}

// newRequestID returns a random nonce for a Request.ID. Android's
// transport doesn't need it to disambiguate channels the way desktop's
// does (see package doc comment), but ipcproto.Request.ID is part of the
// shared wire format, and daemon handlers may still echo/log it, so fill
// it in for consistency with the desktop transport's Call.
func newRequestID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b[:])
}

// Call sends req to the in-process follower daemon serving peerID and
// returns its response. It blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, req ipcproto.Request) (ipcproto.Response, error) {
	req.ID = newRequestID()

	w, fd, err := shmring.CreateAndroidSharedMemory("kvipc-req", capacity)
	if err != nil {
		return ipcproto.Response{}, fmt.Errorf("ipc: create request shm: %w", err)
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

	respChan := make(chan respHandoff, 1)
	select {
	case mailboxFor(peerID) <- call{reqFD: fd, respChan: respChan}:
	case <-ctx.Done():
		w.CloseStorage()
		return ipcproto.Response{}, ctx.Err()
	}

	var rh respHandoff
	select {
	case rh = <-respChan:
	case <-ctx.Done():
		w.CloseStorage()
		return ipcproto.Response{}, ctx.Err()
	}

	r, openErr := shmring.OpenAndroidSharedMemory(rh.fd, capacity)
	close(rh.ack)
	if openErr != nil {
		w.CloseStorage()
		return ipcproto.Response{}, fmt.Errorf("ipc: open response shm: %w", openErr)
	}

	respBuf := make([]byte, ipcproto.ResponseSize)
	if err := readFull(ctx, r, respBuf); err != nil {
		r.Close()
		w.CloseStorage()
		return ipcproto.Response{}, fmt.Errorf("ipc: read response: %w", err)
	}
	r.Close()

	// The response proves the daemon already fully read the request (same
	// reasoning as desktop Call, and unconditionally true here since every
	// round trip gets its own fresh segments -- see package doc comment):
	// safe to release the request segment now.
	w.CloseStorage()

	resp, err := ipcproto.DecodeResponse(respBuf)
	if err != nil {
		return ipcproto.Response{}, err
	}
	return resp, nil
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

// Serve runs the in-process daemon side of the protocol for peerID: it
// repeatedly waits for a Call on the peer's mailbox, dispatches it to
// handle, and hands the response segment's fd back. It blocks until ctx is
// done.
func Serve(ctx context.Context, peerID string, handle Handler) error {
	ch := mailboxFor(peerID)
	for {
		var c call
		select {
		case c = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}

		r, err := shmring.OpenAndroidSharedMemory(c.reqFD, capacity)
		if err != nil {
			continue
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
		resp.ID = req.ID

		w, respFD, err := shmring.CreateAndroidSharedMemory("kvipc-resp", capacity)
		if err != nil {
			continue
		}
		respBuf := resp.Encode()
		if _, err := w.WriteContext(ctx, respBuf[:]); err != nil {
			w.CloseStorage()
			continue
		}
		if err := w.Close(); err != nil {
			w.CloseStorage()
			continue
		}

		ack := make(chan struct{})
		select {
		case c.respChan <- respHandoff{fd: respFD, ack: ack}:
		case <-ctx.Done():
			w.CloseStorage()
			return ctx.Err()
		}
		select {
		case <-ack:
		case <-ctx.Done():
			w.CloseStorage()
			return ctx.Err()
		}
		w.CloseStorage()
	}
}
