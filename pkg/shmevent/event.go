// Package shmevent implements the single wire structure (see
// api/shmevent.capnp) used for every message exchanged between a raft node
// instance and a local "user" -- the desktop CLI, the in-process Android
// UI, or a browser tab's main thread -- over shmring shared memory, and
// (per the same struct's remote counterpart) over
// pkg/daemon.ClientProtocolID. It replaces pkg/ipcproto.
//
// See api/shmevent.capnp's doc comment for the full design rationale: why
// every message carries exactly one raw value plus two relational id
// fields instead of a fixed Key+Value pair, and how Set/Get decompose into
// short sequences of linked messages built around a server-side key
// registry (registry.go).
package shmevent

import (
	"fmt"

	capnp "capnproto.org/go/capnp/v3"
)

// Event type bytes -- the wire values of Msg.EventType. See
// api/shmevent.capnp and this package's doc comment for the
// SetKey/SetField/GetKey/GetField relational pattern.
const (
	// EventSetKey registers Value under this message's own ID in the
	// node's key registry (see registry.go) -- generic, not KV-specific:
	// used both for an actual KV key (ahead of EventSetField) and for a
	// peer id (ahead of EventAdd's learner-join case).
	EventSetKey uint8 = 1
	// EventSetField performs store.Set(registry[SourceID], Value).
	EventSetField uint8 = 2
	// EventGetKey returns registry[SourceID] as Value (reverse lookup).
	EventGetKey uint8 = 3
	// EventGetField performs store.Get(key), where key is
	// registry[SourceID] if SourceID != 0, or Value itself otherwise (a
	// one-shot read needing no prior registry entry).
	EventGetField uint8 = 4
	// EventGetPublicKey returns the node's Ed25519 public key (32 bytes)
	// as Value. Accepted unsigned -- see this package's doc comment.
	EventGetPublicKey uint8 = 5
	// EventGetPrivateKey returns the node's Ed25519 private key (64
	// bytes, stdlib crypto/ed25519 format) as Value. Accepted unsigned --
	// see this package's doc comment.
	EventGetPrivateKey uint8 = 6
	// EventAdd bootstraps this node as the cluster's sole leader (Value
	// empty, SourceID 0), joins as a voter (Value = leader multiaddr to
	// dial, SourceID 0 -- the daemon already knows its own identity, so
	// nothing needs registering first), or -- when SourceID references a
	// prior EventSetKey holding the caller's own peer id -- adds the
	// caller as a non-voting learner at Value (its own reachable
	// address), the shape pkg/daemon.ClientProtocolID's remote browser
	// callers need since the target daemon has no other way to learn a
	// remote caller's identity.
	EventAdd uint8 = 7
	// EventSet is a single-round-trip alternative to the SetKey+SetField
	// pair: Value packs both the key and the value together (see
	// EncodeSetPayload/DecodeSetPayload), so the daemon can perform
	// store.Set(key, value) directly without the registry.Register/
	// Lookup round trip SetKey+SetField needs to relate two separate
	// messages via SourceID. It exists because api/shmevent.capnp's
	// Event has only one `value` field per message -- callers for whom
	// each message doesn't have a meaningful per-round-trip cost (e.g.
	// web-app's Rust client, talking over an already-open network
	// stream) have no reason to switch off SetKey+SetField; EventSet is
	// for pkg/shmclient, which pays a real, non-negligible cost (a fresh
	// shmring segment pair) per round trip. Trade-off: key and value now
	// share one ValueSize (512-byte) budget instead of each getting
	// their own -- SetKey+SetField remains the only option for a
	// combined key+value beyond that.
	EventSet uint8 = 8
	// EventPermitRequest lodges a pending system record -- a request for a
	// peer to be permitted onto the cluster's relay (KindPermitPeer) or
	// for a bootstrap/relay node to be registered (KindBootstrapNode) --
	// under a reserved key (see SystemKey) so every raft member ends up
	// knowing about it purely through ordinary FSM replication, the same
	// as any other Set. Value is EncodePermitRequestPayload(kind, peerID,
	// metadata). Any raft node may receive and relay one (it's applied
	// exactly like EventSet, via handleSetForward's existing one-hop
	// forward-to-leader path) -- see EventPermitConfirm's doc comment for
	// the second stage, which is restricted.
	EventPermitRequest uint8 = 9
	// EventPermitConfirm promotes a pending EventPermitRequest record from
	// pending to confirmed (see SystemKey's Status* values). Value is
	// EncodePermitConfirmPayload(kind, peerID) -- no metadata, since the
	// daemon reads the pending record's own value back out of the store
	// rather than trusting the caller to resend it. Unlike
	// EventPermitRequest, only a peer that is currently a raft *voter* may
	// confirm: any raft node can receive/relay the message, but the
	// daemon's forwarding path re-checks the *libp2p-authenticated*
	// identity of whichever node actually originated it against the
	// leader's live raft configuration before applying -- the per-message
	// Ed25519 signature alone does not prove this (see pkg/daemon's
	// handleForwardConfirmStream doc comment).
	EventPermitConfirm uint8 = 10
	// EventExecute is a direct, unreplicated peer-to-peer notification: it
	// never touches the store or the raft FSM (unlike every event above),
	// it's just delivered straight to whichever node SourceID/
	// DestinationID name. SourceID references a prior EventSetKey holding
	// the *sending* node's own peer id, DestinationID references one
	// holding the *receiving* node's peer id -- both required, no
	// implicit "this node" default the way EventAdd's SourceID==0 case
	// has one. Value is an arbitrary raw payload, up to ValueSize.
	//
	// A node that receives this locally (over pkg/ipc or
	// pkg/daemon.ClientProtocolID) checks that SourceID's registered peer
	// id is genuinely its own (see handleShmEvent) -- it can only ever be
	// relaying on its own behalf, since the peer-to-peer hop that follows
	// is signed with its own key -- then dials DestinationID's peer
	// directly over pkg/daemon.ExecuteProtocolID (a fresh libp2p stream
	// between the two raft node processes, not going through raft
	// consensus at all) and hands it EncodeExecuteNotification(own peer
	// id, Value). The receiving node verifies that message's signature
	// against the sender peer id's own Ed25519 public key (embedded in
	// the peer id itself, the same trick pkg/daemon.recordClusterMember
	// uses) -- self-contained, not dependent on trusting the stream's
	// connection identity -- then queues it for local delivery: see
	// EventPollExecute.
	EventExecute uint8 = 11
	// EventPollExecute drains one queued EventExecute notification (see
	// that event's doc comment) delivered to this node, oldest first.
	// Value is ignored on the request. The response's Value is empty if
	// nothing is queued, or EncodeExecuteNotification(senderPeerID,
	// payload) if one was -- a local caller polls this in a loop (there's
	// no push transport from daemon to pkg/ipc client -- see pkg/ipc's
	// doc comment on why it's strictly request/response) to observe
	// Execute notifications addressed to this node.
	EventPollExecute uint8 = 12
	// EventError is response-only: Value carries a UTF-8 error message,
	// ID echoes the failed request's ID. Not part of the fields the
	// protocol was specified with -- added because the struct has no
	// separate status field, and errors need some way to be reported;
	// see this package's doc comment.
	EventError uint8 = 255
)

// EventName returns a human-readable name for e, for logging -- "unknown"
// for anything not defined above.
func EventName(e uint8) string {
	switch e {
	case EventSetKey:
		return "set_key"
	case EventSetField:
		return "set_field"
	case EventGetKey:
		return "get_key"
	case EventGetField:
		return "get_field"
	case EventGetPublicKey:
		return "get_public_key"
	case EventGetPrivateKey:
		return "get_private_key"
	case EventAdd:
		return "add"
	case EventSet:
		return "set"
	case EventPermitRequest:
		return "permit_request"
	case EventPermitConfirm:
		return "permit_confirm"
	case EventExecute:
		return "execute"
	case EventPollExecute:
		return "poll_execute"
	case EventError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

// EventFromName is the inverse of EventName: it returns the event type byte
// for one of the names EventName produces ("set_key", "set_field", ...),
// and false if name isn't recognized. Used to parse a human-authored event
// name (e.g. from test/e2e/testdata.json or kvctl-cli sendevent's JSON
// argument) back into the wire byte.
func EventFromName(name string) (uint8, bool) {
	switch name {
	case "set_key":
		return EventSetKey, true
	case "set_field":
		return EventSetField, true
	case "get_key":
		return EventGetKey, true
	case "get_field":
		return EventGetField, true
	case "get_public_key":
		return EventGetPublicKey, true
	case "get_private_key":
		return EventGetPrivateKey, true
	case "add":
		return EventAdd, true
	case "set":
		return EventSet, true
	case "permit_request":
		return EventPermitRequest, true
	case "permit_confirm":
		return EventPermitConfirm, true
	case "execute":
		return EventExecute, true
	case "poll_execute":
		return EventPollExecute, true
	case "error":
		return EventError, true
	default:
		return 0, false
	}
}

// ValueSize is the maximum length of Msg.Value this package enforces (a
// convention, not a capnp schema constraint -- see api/shmevent.capnp's
// doc comment on Event.value).
const ValueSize = 512

// SignatureSize is the length of an Ed25519 signature.
const SignatureSize = 64

// PublicKeySize is the length of an Ed25519 public key.
const PublicKeySize = 32

// PrivateKeySize is the length of an Ed25519 private key in stdlib
// crypto/ed25519's format (32-byte seed + 32-byte public key).
const PrivateKeySize = 64

// Msg is the Go-friendly form of the capnp Event struct (named Msg, not
// Event, only to avoid colliding with the generated capnp type of that
// name in this same package -- see shmevent.capnp.go): Encode/Decode
// handle capnp (de)serialization plus CRC/signature computation and
// verification, so callers never touch the generated capnp API directly.
type Msg struct {
	EventType     uint8
	SourceID      uint16
	DestinationID uint16
	Value         []byte
	ID            uint16
}

// EncodeSetPayload packs key and value into a single EventSet Msg.Value: a
// 2-byte big-endian length prefix for key, then key verbatim, then value
// verbatim -- the rest of the buffer, with no length prefix of its own,
// since Value's own end marks value's end. See EventSet's doc comment for
// why this packing exists instead of a second capnp field.
func EncodeSetPayload(key, value []byte) ([]byte, error) {
	if len(key) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: set payload key too long: %d bytes", len(key))
	}
	buf := make([]byte, 2+len(key)+len(value))
	buf[0] = byte(len(key) >> 8)
	buf[1] = byte(len(key))
	copy(buf[2:], key)
	copy(buf[2+len(key):], value)
	return buf, nil
}

// DecodeSetPayload is the inverse of EncodeSetPayload.
func DecodeSetPayload(payload []byte) (key, value []byte, err error) {
	if len(payload) < 2 {
		return nil, nil, fmt.Errorf("shmevent: set payload too short: %d bytes", len(payload))
	}
	keyLen := int(payload[0])<<8 | int(payload[1])
	if 2+keyLen > len(payload) {
		return nil, nil, fmt.Errorf("shmevent: set payload key length %d exceeds payload size %d", keyLen, len(payload))
	}
	return payload[2 : 2+keyLen], payload[2+keyLen:], nil
}

// EncodeExecuteNotification packs senderPeerID and payload into a single
// value: a 2-byte big-endian length prefix for senderPeerID, then
// senderPeerID verbatim, then payload -- the rest of the buffer, with no
// length prefix of its own, mirroring EncodeSetPayload exactly. Used both
// for the wire message pkg/daemon.ExecuteProtocolID's handler sends (where
// SourceID/DestinationID have no cross-node meaning, so the sender's peer
// id travels in Value instead) and for EventPollExecute's response, so a
// local caller draining its queue learns who an Execute notification came
// from.
func EncodeExecuteNotification(senderPeerID, payload []byte) ([]byte, error) {
	if len(senderPeerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: execute notification sender peer id too long: %d bytes", len(senderPeerID))
	}
	buf := make([]byte, 2+len(senderPeerID)+len(payload))
	buf[0] = byte(len(senderPeerID) >> 8)
	buf[1] = byte(len(senderPeerID))
	copy(buf[2:], senderPeerID)
	copy(buf[2+len(senderPeerID):], payload)
	return buf, nil
}

// DecodeExecuteNotification is the inverse of EncodeExecuteNotification.
func DecodeExecuteNotification(data []byte) (senderPeerID, payload []byte, err error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("shmevent: execute notification too short: %d bytes", len(data))
	}
	idLen := int(data[0])<<8 | int(data[1])
	if 2+idLen > len(data) {
		return nil, nil, fmt.Errorf("shmevent: execute notification sender peer id length %d exceeds payload size %d", idLen, len(data))
	}
	return data[2 : 2+idLen], data[2+idLen:], nil
}

// Encode serializes m to its capnp wire form, computing CRC32 and signing
// with priv. priv may be nil only for EventGetPublicKey/EventGetPrivateKey
// requests (see Sign's doc comment).
func Encode(m Msg, priv PrivateKey) ([]byte, error) {
	if len(m.Value) > ValueSize {
		return nil, fmt.Errorf("shmevent: value too long: %d bytes (max %d)", len(m.Value), ValueSize)
	}

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("shmevent: new message: %w", err)
	}
	root, err := NewRootEvent(seg)
	if err != nil {
		return nil, fmt.Errorf("shmevent: new root: %w", err)
	}
	root.SetEvent(m.EventType)
	root.SetSourceId(m.SourceID)
	root.SetDestinationId(m.DestinationID)
	if err := root.SetValue(m.Value); err != nil {
		return nil, fmt.Errorf("shmevent: set value: %w", err)
	}
	root.SetId(m.ID)

	crc := crc32Of(m)
	root.SetCrc32(crc)

	sig, err := Sign(priv, m, crc)
	if err != nil {
		return nil, fmt.Errorf("shmevent: sign: %w", err)
	}
	if err := root.SetSignature(sig); err != nil {
		return nil, fmt.Errorf("shmevent: set signature: %w", err)
	}

	return msg.Marshal()
}

// Decode parses buf as a capnp Event message and verifies its CRC32
// against the decoded fields. It does not verify the signature -- callers
// that need authenticity (anything but a bootstrap
// GetPublicKey/GetPrivateKey exchange) must call Verify explicitly once
// they know which public key to check against; see this package's doc
// comment on why those two events are the exception.
func Decode(buf []byte) (m Msg, crc uint32, signature []byte, err error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: unmarshal: %w", err)
	}
	root, err := ReadRootEvent(msg)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: read root: %w", err)
	}
	value, err := root.Value()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: value: %w", err)
	}
	sig, err := root.Signature()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: signature: %w", err)
	}

	m = Msg{
		EventType:     root.Event(),
		SourceID:      root.SourceId(),
		DestinationID: root.DestinationId(),
		Value:         append([]byte(nil), value...),
		ID:            root.Id(),
	}
	wantCRC := root.Crc32()
	if gotCRC := crc32Of(m); gotCRC != wantCRC {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: crc32 mismatch: got %#x, message says %#x", gotCRC, wantCRC)
	}
	return m, wantCRC, append([]byte(nil), sig...), nil
}
