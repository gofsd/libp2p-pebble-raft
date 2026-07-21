package shmevent

import "fmt"

// SystemKeyPrefix marks a pkg/store key as reserved for internal cluster
// bookkeeping -- permitted peers, bootstrap nodes, and (planned, not yet
// implemented) raft voter/learner add/remove operations -- rather than
// user data. pkg/daemon's EventSetField/EventSet cases reject any
// caller-supplied key starting with this byte, reserving the entire
// namespace for SystemKey's own use.
const SystemKeyPrefix = 0x00

// Kind bytes -- what a system record (see SystemKey) is about. Values
// 0x05 and above are still unassigned, reserved for future system
// operations built on this same EventPermitRequest/EventPermitConfirm
// two-stage workflow (e.g. a raft voter adding or removing another raft
// voter/learner), so that work can slot in without changing this key
// layout.
const (
	KindPermitPeer    byte = 0x01 // permission for a peer to join/use the cluster's relay
	KindBootstrapNode byte = 0x02 // registration of a stable relay/bootstrap point
	// KindClusterMember records a raft member's public key and current
	// role (RoleVoter/RoleLearner/RoleLeader) -- see ClusterMemberKey/
	// EncodeClusterMemberPayload. Unlike KindPermitPeer/KindBootstrapNode
	// it has no pending/confirmed two-stage lifecycle: it's a live status
	// mirror, always written directly under a fixed status placeholder
	// (see ClusterMemberKey), kept current by pkg/daemon whenever a peer
	// joins or this node's own raft leadership status changes.
	KindClusterMember byte = 0x03
	// KindLogPermit consumes the 0x04 slot this block used to leave
	// unassigned: permission for a peer to append/query pkg/logrecord
	// records of one specific log kind. Unlike KindPermitPeer -- which
	// keys purely on peerID -- a log-kind permit record needs a second
	// variable-length dimension (which log kind string), which
	// SystemKey's fixed 3-field shape (prefix, kind, status, then just
	// peerID) can't express; see LogPermitKey, this package's own key
	// builder for that shape, kept here since checkSystemListCap
	// (pkg/kvfsm) and the voter-gated confirm/revoke machinery
	// (pkg/daemon's handleConfirmForward et al.) both key off
	// SystemKeyPrefix, and this record needs both.
	KindLogPermit byte = 0x04
	// KindClusterJoin consumes the 0x05 slot this block's own doc comment
	// reserved for exactly this: a raft voter/learner add, built on the
	// same EventPermitRequest/EventPermitConfirm/EventPermitRevoke
	// pending->confirmed lifecycle as KindPermitPeer, rather than a new
	// wire protocol. metadata (see EncodeClusterJoinMetadata) carries the
	// joining peer's dialable multiaddr and requested suffrage; a
	// confirm promotes the pending record to confirmed exactly like any
	// other kind, but pkg/daemon's applyConfirm additionally special-
	// cases this one kind to actually call raft.AddVoter/AddNonvoter at
	// that moment (see applyConfirm's doc comment) -- everywhere else in
	// this package and pkg/kvfsm, KindClusterJoin is opaque, handled
	// identically to any other kind.
	KindClusterJoin byte = 0x05
)

// KindName returns a human-readable name for k, for CLI use (mage/
// kvctl-cli's requestpermit/confirmpermit commands) -- "unknown(N)" for
// anything not defined above. Mirrors EventName/EventFromName.
func KindName(k byte) string {
	switch k {
	case KindPermitPeer:
		return "peer"
	case KindBootstrapNode:
		return "bootstrap"
	case KindClusterMember:
		return "cluster-member"
	case KindClusterJoin:
		return "cluster-join"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// KindFromName is the inverse of KindName: it returns the kind byte for
// one of the names KindName produces ("peer", "bootstrap",
// "cluster-member", "cluster-join"), and false if name isn't recognized.
func KindFromName(name string) (byte, bool) {
	switch name {
	case "peer":
		return KindPermitPeer, true
	case "bootstrap":
		return KindBootstrapNode, true
	case "cluster-member":
		return KindClusterMember, true
	case "cluster-join":
		return KindClusterJoin, true
	default:
		return 0, false
	}
}

// Role bytes -- a raft member's current standing, as recorded in a
// KindClusterMember record's payload (see EncodeClusterMemberPayload).
const (
	RoleVoter   byte = 0x01
	RoleLearner byte = 0x02
	RoleLeader  byte = 0x03
)

// Status bytes -- where a system record is in its two-stage
// request/confirm lifecycle (see SystemKey).
const (
	StatusPending   byte = 0x01
	StatusConfirmed byte = 0x02
)

// Suffrage bytes -- the raft membership shape a KindClusterJoin request
// asks for, packed into EncodeClusterJoinMetadata's payload. Mirrors
// pkg/daemon's parseJoinRequest "voter"/"learner" wire tokens (raft.Voter/
// raft.Nonvoter) in a form this package can encode without importing
// hashicorp/raft.
const (
	SuffrageVoter   byte = 0x01
	SuffrageLearner byte = 0x02
)

// EncodeClusterJoinMetadata packs a joining peer's dialable multiaddr and
// requested suffrage into the metadata argument EventPermitRequest expects
// (see EncodePermitRequestPayload) for a KindClusterJoin request: suffrage
// byte first (fixed size), then addr verbatim as the rest of the buffer --
// no length prefix needed since addr is always last.
func EncodeClusterJoinMetadata(addr string, suffrage byte) []byte {
	buf := make([]byte, 1+len(addr))
	buf[0] = suffrage
	copy(buf[1:], addr)
	return buf
}

// DecodeClusterJoinMetadata is the inverse of EncodeClusterJoinMetadata.
func DecodeClusterJoinMetadata(payload []byte) (addr string, suffrage byte, err error) {
	if len(payload) < 1 {
		return "", 0, fmt.Errorf("shmevent: cluster join metadata too short: %d bytes", len(payload))
	}
	return string(payload[1:]), payload[0], nil
}

// SystemKey builds the pkg/store key for a system record:
// SystemKeyPrefix, kind, status, then peerID verbatim. peerID is always
// the last field, so its length needs no prefix here (contrast
// EncodePermitRequestPayload, where something follows it).
func SystemKey(kind, status byte, peerID []byte) []byte {
	key := make([]byte, 3+len(peerID))
	key[0] = SystemKeyPrefix
	key[1] = kind
	key[2] = status
	copy(key[3:], peerID)
	return key
}

// clusterMemberStatusPlaceholder is the fixed status byte ClusterMemberKey
// uses -- KindClusterMember has no pending/confirmed lifecycle (see that
// constant's doc comment), so unlike SystemKey's other callers this isn't
// StatusPending/StatusConfirmed, just a placeholder keeping the key layout
// uniform with every other SystemKey-produced key.
const clusterMemberStatusPlaceholder = 0x00

// ClusterMemberKey builds the pkg/store key for peerID's KindClusterMember
// record -- see that constant's doc comment.
func ClusterMemberKey(peerID []byte) []byte {
	return SystemKey(KindClusterMember, clusterMemberStatusPlaceholder, peerID)
}

// EncodeClusterMemberPayload packs pub and role into a single
// KindClusterMember record's value: pub is always exactly
// ed25519.PublicKeySize (32) bytes, role is the trailing byte -- both
// fixed-size, so unlike EncodePermitRequestPayload no length prefix is
// needed.
func EncodeClusterMemberPayload(pub PublicKey, role byte) []byte {
	buf := make([]byte, len(pub)+1)
	copy(buf, pub)
	buf[len(pub)] = role
	return buf
}

// DecodeClusterMemberPayload is the inverse of EncodeClusterMemberPayload.
func DecodeClusterMemberPayload(payload []byte) (pub PublicKey, role byte, err error) {
	if len(payload) != PublicKeySize+1 {
		return nil, 0, fmt.Errorf("shmevent: cluster member payload must be %d bytes, got %d", PublicKeySize+1, len(payload))
	}
	pub = PublicKey(payload[:PublicKeySize])
	role = payload[PublicKeySize]
	return pub, role, nil
}

// EncodePermitRequestPayload packs kind, peerID, and metadata into a
// single EventPermitRequest Msg.Value: kind, then a 2-byte big-endian
// length prefix for peerID, then peerID, then metadata verbatim (the
// rest of the buffer, needing no length prefix of its own). metadata is
// opaque to this package -- e.g. a dialable multiaddr for
// KindBootstrapNode, or empty for KindPermitPeer.
func EncodePermitRequestPayload(kind byte, peerID, metadata []byte) ([]byte, error) {
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: permit request peerID too long: %d bytes", len(peerID))
	}
	buf := make([]byte, 1+2+len(peerID)+len(metadata))
	buf[0] = kind
	buf[1] = byte(len(peerID) >> 8)
	buf[2] = byte(len(peerID))
	copy(buf[3:], peerID)
	copy(buf[3+len(peerID):], metadata)
	return buf, nil
}

// DecodePermitRequestPayload is the inverse of EncodePermitRequestPayload.
func DecodePermitRequestPayload(payload []byte) (kind byte, peerID, metadata []byte, err error) {
	if len(payload) < 3 {
		return 0, nil, nil, fmt.Errorf("shmevent: permit request payload too short: %d bytes", len(payload))
	}
	kind = payload[0]
	idLen := int(payload[1])<<8 | int(payload[2])
	if 3+idLen > len(payload) {
		return 0, nil, nil, fmt.Errorf("shmevent: permit request peerID length %d exceeds payload size %d", idLen, len(payload))
	}
	return kind, payload[3 : 3+idLen], payload[3+idLen:], nil
}

// EncodePermitConfirmPayload packs kind and peerID (the rest of the
// buffer) into a single EventPermitConfirm Msg.Value -- no metadata
// field, since the daemon reads the pending request's own value back out
// of the store rather than trusting the confirming caller to resend it.
func EncodePermitConfirmPayload(kind byte, peerID []byte) []byte {
	buf := make([]byte, 1+len(peerID))
	buf[0] = kind
	copy(buf[1:], peerID)
	return buf
}

// DecodePermitConfirmPayload is the inverse of EncodePermitConfirmPayload.
func DecodePermitConfirmPayload(payload []byte) (kind byte, peerID []byte, err error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("shmevent: permit confirm payload too short")
	}
	return payload[0], payload[1:], nil
}
