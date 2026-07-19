// Package logrecord implements a generic, append-only structured record
// store on top of pkg/store, replicated the ordinary way through raft
// (see pkg/shmevent.EventLogAppend, a thin wrapper around the same
// handleSetForward path EventSet uses) and queryable by kind/unit/time
// window via pkg/shmevent.EventListRange.
//
// This is deliberately generic: `kind` is any caller-chosen string, not a
// fixed enum -- a journal, a situation report, or any other record type
// is defined purely by what string a caller passes when appending, with
// no code change needed to add a new one. Record itself is likewise a
// generic envelope (see record.go), not an implementation of any
// standardized report format's real field layout.
package logrecord

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

// LogKeyPrefix marks a pkg/store key as belonging to this package --
// reserved the same way pkg/shmevent.SystemKeyPrefix (0x00) reserves its
// own namespace, but as its own sibling top-level byte rather than a kind
// inside SystemKeyPrefix's space: SystemKeyPrefix's kind byte is reserved
// specifically for the permit/confirm two-stage ACL workflow (see
// pkg/shmevent/system.go's doc comment on that reservation), which
// records here have no part in. pkg/daemon's EventSetField/EventSet
// handling rejects any caller-supplied key starting with this byte, the
// same way it already does for SystemKeyPrefix, so an ordinary Set can
// never collide with (or corrupt) a log record.
const LogKeyPrefix = 0x01

// maxFieldLen bounds kind and unitID, mirroring
// pkg/shmevent.EncodePermitRequestPayload's identical bound on peerID:
// both get a 2-byte big-endian length prefix in the key (see BuildKey),
// so neither can exceed what that prefix can express.
const maxFieldLen = 0xFFFF

// RandSize is the width of BuildKey's rand field, in bytes.
const RandSize = 8

// BuildKey packs kind, unitID, ts, and rand into a single pkg/store key:
//
//	[LogKeyPrefix][kindLen 2BE][kind][unitIDLen 2BE][unitID][tsNano 8BE][rand]
//
// kind and unitID both need their own length prefix (unlike
// pkg/shmevent.SystemKey, which puts its one variable-length field,
// peerID, last with no prefix at all) because neither is the last field
// here and neither is fixed-width -- without a length prefix, kind
// "sitrep" and kind "sitrep2" (or unit "1BCT" and unit "1BCT2") would
// have keys where one is a byte-prefix of the other, corrupting any
// range scan bounded to just one of them. The length prefix makes the
// keys diverge at the length byte itself, before the shared prefix
// bytes -- see ScanBounds/KindPrefix for why that property matters.
//
// tsNano (ts.UnixNano(), big-endian) placed right after the
// length-prefixed kind+unit block is what makes "every record of this
// kind and unit, in chronological order" a plain byte-wise range scan
// (pkg/store's SQLite backend compares BLOB keys byte-wise). rand (see
// RandSize) is a fixed-width tiebreaker appended last so two records
// with the same kind/unit/timestamp still get distinct keys without
// needing a coordinated sequence number -- BuildKey and the whole append
// path stay a single round trip, no read-before-write.
func BuildKey(kind, unitID string, ts time.Time, rnd [RandSize]byte) ([]byte, error) {
	if len(kind) > maxFieldLen {
		return nil, fmt.Errorf("logrecord: kind too long: %d bytes", len(kind))
	}
	if len(unitID) > maxFieldLen {
		return nil, fmt.Errorf("logrecord: unitID too long: %d bytes", len(unitID))
	}
	buf := make([]byte, 1+2+len(kind)+2+len(unitID)+8+RandSize)
	off := 0
	buf[off] = LogKeyPrefix
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(kind)))
	off += 2
	off += copy(buf[off:], kind)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(unitID)))
	off += 2
	off += copy(buf[off:], unitID)
	binary.BigEndian.PutUint64(buf[off:], uint64(ts.UnixNano()))
	off += 8
	copy(buf[off:], rnd[:])
	return buf, nil
}

// NewRand returns a fresh random BuildKey tiebreaker, sourced from
// crypto/rand.
func NewRand() ([RandSize]byte, error) {
	var r [RandSize]byte
	_, err := rand.Read(r[:])
	return r, err
}

// ParseKey is the inverse of BuildKey.
func ParseKey(key []byte) (kind, unitID string, ts time.Time, err error) {
	if len(key) < 1 || key[0] != LogKeyPrefix {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key missing LogKeyPrefix")
	}
	off := 1
	if off+2 > len(key) {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key truncated before kind length")
	}
	kindLen := int(binary.BigEndian.Uint16(key[off:]))
	off += 2
	if off+kindLen > len(key) {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key truncated in kind")
	}
	kind = string(key[off : off+kindLen])
	off += kindLen
	if off+2 > len(key) {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key truncated before unitID length")
	}
	unitIDLen := int(binary.BigEndian.Uint16(key[off:]))
	off += 2
	if off+unitIDLen > len(key) {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key truncated in unitID")
	}
	unitID = string(key[off : off+unitIDLen])
	off += unitIDLen
	if off+8 > len(key) {
		return "", "", time.Time{}, fmt.Errorf("logrecord: key truncated before timestamp")
	}
	ts = time.Unix(0, int64(binary.BigEndian.Uint64(key[off:])))
	return kind, unitID, ts, nil
}

// kindUnitPrefix returns the fixed key prefix shared by every record of
// the given kind and unitID, up to (not including) the timestamp field --
// the common building block behind KindPrefix and ScanBounds.
func kindUnitPrefix(kind, unitID string) []byte {
	buf := make([]byte, 1+2+len(kind)+2+len(unitID))
	off := 0
	buf[off] = LogKeyPrefix
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(kind)))
	off += 2
	off += copy(buf[off:], kind)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(unitID)))
	off += 2
	copy(buf[off:], unitID)
	return buf
}

// ScanBounds returns the inclusive [lo, hi] key range covering every
// record of the given kind and unitID with a timestamp in [start, end]
// -- suitable to pass directly to pkg/store.Store.ScanRange (or
// pkg/shmevent.EncodeListRangeQuery for a remote/IPC caller). hi pads
// with a run of 0xFF bytes past the timestamp field so it sorts after
// any rand suffix a record with timestamp == end could have, making the
// range genuinely inclusive of end down to the nanosecond.
func ScanBounds(kind, unitID string, start, end time.Time) (lo, hi []byte) {
	prefix := kindUnitPrefix(kind, unitID)

	lo = make([]byte, len(prefix)+8)
	copy(lo, prefix)
	binary.BigEndian.PutUint64(lo[len(prefix):], uint64(start.UnixNano()))

	hi = make([]byte, len(prefix)+8+RandSize)
	copy(hi, prefix)
	binary.BigEndian.PutUint64(hi[len(prefix):], uint64(end.UnixNano()))
	for i := len(prefix) + 8; i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// KindPrefix returns the key prefix shared by every record of the given
// kind, across every unitID -- suitable for pkg/store.Store.ScanPrefix,
// e.g. to enumerate the distinct unit IDs that have ever logged a record
// of this kind (decode each returned key with ParseKey).
func KindPrefix(kind string) []byte {
	buf := make([]byte, 1+2+len(kind))
	buf[0] = LogKeyPrefix
	binary.BigEndian.PutUint16(buf[1:], uint16(len(kind)))
	copy(buf[3:], kind)
	return buf
}

// AllPrefix returns the key prefix shared by every record of every kind
// and unit -- suitable for pkg/store.Store.ScanPrefix to enumerate the
// distinct kinds that have ever been logged (decode each returned key
// with ParseKey), the config-discovery use case this package is built
// around: a caller need not know the set of valid kinds in advance.
func AllPrefix() []byte {
	return []byte{LogKeyPrefix}
}
