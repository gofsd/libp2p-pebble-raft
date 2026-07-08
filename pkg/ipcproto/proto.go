// Package ipcproto defines the fixed-size wire messages exchanged between
// the mage CLI (client) and a running kvnode daemon (server) over the
// shmring-backed local IPC channel implemented in pkg/ipc.
//
// Messages are fixed size so a single shmring Write/Read maps to exactly one
// message, with no separate length framing needed.
package ipcproto

import (
	"bytes"
	"fmt"
)

// Action identifies what the daemon should do with a Request.
type Action uint8

const (
	// ActionAdd bootstraps a freshly spawned daemon process: Key carries the
	// leader's peer id to join ("" if this node is itself to become the
	// cluster's initial leader), Value carries this node's own peer id, used
	// by the daemon only to double check it was started with the identity
	// the caller expected.
	ActionAdd Action = 1
	// ActionSet stores Key=Value through raft.
	ActionSet Action = 2
	// ActionGet reads the current value of Key.
	ActionGet Action = 3
)

func (a Action) String() string {
	switch a {
	case ActionAdd:
		return "add"
	case ActionSet:
		return "set"
	case ActionGet:
		return "get"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(a))
	}
}

const (
	// KeySize is the fixed width of the Request.Key field, in bytes.
	KeySize = 256
	// ValueSize is the fixed width of the Request.Value and Response.Value
	// fields, in bytes.
	ValueSize = 256

	// RequestSize is the total encoded size of a Request.
	RequestSize = 1 + KeySize + ValueSize
	// ResponseSize is the total encoded size of a Response.
	ResponseSize = 1 + ValueSize
)

// Request is the fixed-size message the CLI sends to the daemon.
type Request struct {
	Action Action
	Key    [KeySize]byte
	Value  [ValueSize]byte
}

// NewRequest builds a Request from Go strings, truncating key/value to their
// field widths if necessary.
func NewRequest(action Action, key, value string) Request {
	var req Request
	req.Action = action
	putString(req.Key[:], key)
	putString(req.Value[:], value)
	return req
}

// Encode serializes req to its wire form.
func (req Request) Encode() [RequestSize]byte {
	var buf [RequestSize]byte
	buf[0] = byte(req.Action)
	copy(buf[1:1+KeySize], req.Key[:])
	copy(buf[1+KeySize:], req.Value[:])
	return buf
}

// DecodeRequest parses a wire-form Request. buf must be at least RequestSize
// bytes.
func DecodeRequest(buf []byte) (Request, error) {
	if len(buf) < RequestSize {
		return Request{}, fmt.Errorf("ipcproto: short request: %d bytes", len(buf))
	}
	var req Request
	req.Action = Action(buf[0])
	copy(req.Key[:], buf[1:1+KeySize])
	copy(req.Value[:], buf[1+KeySize:1+KeySize+ValueSize])
	return req, nil
}

// KeyString returns Key with trailing zero padding trimmed.
func (req Request) KeyString() string { return getString(req.Key[:]) }

// ValueString returns Value with trailing zero padding trimmed.
func (req Request) ValueString() string { return getString(req.Value[:]) }

// Status is the outcome of a Request as reported in a Response.
type Status uint8

const (
	StatusOK    Status = 0
	StatusError Status = 1
)

// Response is the fixed-size message the daemon sends back to the CLI.
type Response struct {
	Status Status
	// Value carries the requested value (ActionGet), the confirmed/assigned
	// peer id (ActionAdd), or a truncated error message (Status == StatusError).
	Value [ValueSize]byte
}

// NewResponse builds a Response from a Go string, truncating to ValueSize if
// necessary.
func NewResponse(status Status, value string) Response {
	var resp Response
	resp.Status = status
	putString(resp.Value[:], value)
	return resp
}

// Encode serializes resp to its wire form.
func (resp Response) Encode() [ResponseSize]byte {
	var buf [ResponseSize]byte
	buf[0] = byte(resp.Status)
	copy(buf[1:], resp.Value[:])
	return buf
}

// DecodeResponse parses a wire-form Response. buf must be at least
// ResponseSize bytes.
func DecodeResponse(buf []byte) (Response, error) {
	if len(buf) < ResponseSize {
		return Response{}, fmt.Errorf("ipcproto: short response: %d bytes", len(buf))
	}
	var resp Response
	resp.Status = Status(buf[0])
	copy(resp.Value[:], buf[1:1+ValueSize])
	return resp, nil
}

// ValueString returns Value with trailing zero padding trimmed.
func (resp Response) ValueString() string { return getString(resp.Value[:]) }

func putString(dst []byte, s string) {
	n := copy(dst, s)
	for i := n; i < len(dst); i++ {
		dst[i] = 0
	}
}

func getString(src []byte) string {
	i := bytes.IndexByte(src, 0)
	if i < 0 {
		return string(src)
	}
	return string(src[:i])
}
