// Package shmclient implements the caller-side orchestration for
// pkg/shmevent's relational protocol over pkg/ipc: the SetKey+SetField
// message pair a Set needs, the single inline-key GetField a one-shot Get
// needs, and the GetPrivateKey bootstrap every signed call needs first
// (see pkg/shmevent's doc comment on why a local caller signs with the
// same Ed25519 key the node's own identity uses). Used by pkg/kvctl (the
// desktop CLI) and mobile/kvmobile (the in-process Android UI) -- anything
// that drives a node over pkg/ipc rather than pkg/shmevent's wire struct
// directly (as web-app's Rust build does, over ClientProtocolID).
package shmclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Session caches the signing key fetched from peerID for the lifetime of
// the caller's own process/session, so repeated Set/Get/Add calls only
// pay the GetPrivateKey round trip once -- important for a long-lived
// caller like mobile/kvmobile, less so for pkg/kvctl's short-lived CLI
// invocations, which still go through it via the package-level
// convenience functions below.
type Session struct {
	peerID string
	priv   shmevent.PrivateKey
}

// Open fetches peerID's signing key (see pkg/shmevent's doc comment on why
// this is safe/expected for a local, same-machine caller) and returns a
// Session ready for Set/Get/Add.
func Open(ctx context.Context, peerID string) (*Session, error) {
	priv, err := GetPrivateKey(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("shmclient: fetch signing key: %w", err)
	}
	return &Session{peerID: peerID, priv: priv}, nil
}

// newID returns a random non-zero id for a new message -- 0 is reserved
// meaning "SourceID/DestinationID not used" (see api/shmevent.capnp), so a
// real message's own id avoids it too, even though nothing currently cites
// these particular ids by SourceID.
func newID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}

// Set applies key=value through raft on the session's node, in a single
// EventSet round trip (key and value packed together via
// shmevent.EncodeSetPayload) rather than the SetKey+SetField pair --
// see EventSet's doc comment for why: pkg/ipc.Call pays a real,
// non-negligible cost (a fresh shmring segment pair) per round trip, so a
// caller in this package's position halves Set's cost by not needing two.
func (s *Session) Set(ctx context.Context, key, value string) error {
	payload, err := shmevent.EncodeSetPayload([]byte(key), []byte(value))
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventSet,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: set: %s", resp.Value)
	}
	return nil
}

// Get reads key from the session's node -- a one-shot GetField carrying
// key directly in Value (SourceID left 0), skipping the registry
// round-trip Set needs -- which, like any raft follower's local read, may
// lag a moment behind a Set that just committed on the leader.
func (s *Session) Get(ctx context.Context, key string) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventGetField,
		Value:     []byte(key),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: get_field: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: get_field: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// Add bootstraps this node as the cluster's sole leader (leaderPeerID ==
// "") or joins it to the cluster led by leaderPeerID as a voter. Returns
// this node's own peer id, mirroring the pre-shmevent
// ipcproto.ActionAdd response. See pkg/shmevent.EventAdd's doc comment for
// the (unused here) learner-join shape a remote browser caller uses
// instead.
func (s *Session) Add(ctx context.Context, leaderPeerID string) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventAdd,
		Value:     []byte(leaderPeerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: add: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: add: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// RequestPermit lodges a pending permit request for peerID (of the given
// kind -- shmevent.KindPermitPeer or shmevent.KindBootstrapNode) on the
// session's node. metadata is opaque, kind-specific data (e.g. a dialable
// multiaddr for KindBootstrapNode). See shmevent.EventPermitRequest's doc
// comment: any raft node may receive and relay this.
func (s *Session) RequestPermit(ctx context.Context, kind byte, peerID, metadata []byte) error {
	payload, err := shmevent.EncodePermitRequestPayload(kind, peerID, metadata)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPermitRequest,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: permit_request: %s", resp.Value)
	}
	return nil
}

// ConfirmPermit promotes a pending permit request for peerID (of the
// given kind) from pending to confirmed. See
// shmevent.EventPermitConfirm's doc comment: only a peer that is
// currently a raft voter may confirm -- the session's node will reject
// this (surfaced as an error here) if it forwards to a leader that
// determines the confirming node isn't one.
func (s *Session) ConfirmPermit(ctx context.Context, kind byte, peerID []byte) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPermitConfirm,
		Value:     shmevent.EncodePermitConfirmPayload(kind, peerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: permit_confirm: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: permit_confirm: %s", resp.Value)
	}
	return nil
}

// GetPublicKey fetches peerID's Ed25519 public key -- unsigned, since it's
// one of the two bootstrap events a node accepts without a key to check a
// signature against yet (see pkg/shmevent.RequiresSignature).
func GetPublicKey(ctx context.Context, peerID string) (shmevent.PublicKey, error) {
	resp, err := ipc.Call(ctx, peerID, shmevent.Msg{
		EventType: shmevent.EventGetPublicKey,
		ID:        newID(),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_public_key: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, fmt.Errorf("shmclient: get_public_key: %s", resp.Value)
	}
	return shmevent.PublicKey(resp.Value), nil
}

// GetPrivateKey fetches peerID's Ed25519 private key -- unsigned, same
// bootstrap exception as GetPublicKey.
func GetPrivateKey(ctx context.Context, peerID string) (shmevent.PrivateKey, error) {
	resp, err := ipc.Call(ctx, peerID, shmevent.Msg{
		EventType: shmevent.EventGetPrivateKey,
		ID:        newID(),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_private_key: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, fmt.Errorf("shmclient: get_private_key: %s", resp.Value)
	}
	return shmevent.PrivateKey(resp.Value), nil
}

// Set is a one-shot convenience wrapper around Open+Session.Set, for a
// short-lived caller (pkg/kvctl) that doesn't need to cache the session
// across multiple calls.
func Set(ctx context.Context, peerID, key, value string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, value)
}

// Get is the one-shot convenience wrapper around Open+Session.Get.
func Get(ctx context.Context, peerID, key string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Get(ctx, key)
}

// Add is the one-shot convenience wrapper around Open+Session.Add.
//
// Bootstrap/first-join is a special case: a brand new node has no signing
// key exchange to do beyond what Open already performs (GetPrivateKey is
// itself unsigned and always available), so this works uniformly whether
// or not the node has ever been added to a cluster before.
func Add(ctx context.Context, peerID, leaderPeerID string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Add(ctx, leaderPeerID)
}

// RequestPermit is the one-shot convenience wrapper around
// Open+Session.RequestPermit.
func RequestPermit(ctx context.Context, peerID string, kind byte, targetPeerID, metadata []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.RequestPermit(ctx, kind, targetPeerID, metadata)
}

// ConfirmPermit is the one-shot convenience wrapper around
// Open+Session.ConfirmPermit.
func ConfirmPermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.ConfirmPermit(ctx, kind, targetPeerID)
}
