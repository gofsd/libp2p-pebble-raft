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
// 0x03 and above are intentionally left unassigned: they're reserved for
// future system operations built on this same EventPermitRequest/
// EventPermitConfirm two-stage workflow (e.g. a raft voter adding or
// removing another raft voter/learner), so that work can slot in without
// changing this key layout.
const (
	KindPermitPeer    byte = 0x01 // permission for a peer to join/use the cluster's relay
	KindBootstrapNode byte = 0x02 // registration of a stable relay/bootstrap point
)

// Status bytes -- where a system record is in its two-stage
// request/confirm lifecycle (see SystemKey).
const (
	StatusPending   byte = 0x01
	StatusConfirmed byte = 0x02
)

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
