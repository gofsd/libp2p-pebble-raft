package logrecord

import (
	"encoding/json"
	"time"
)

// Record is the generic envelope every logged entry is stored as (JSON
// -encoded, see MarshalJSON/UnmarshalJSON): common metadata plus two
// entirely caller-defined fields, Fields and Narrative. This package
// makes no claim to implementing any real report format's actual
// standardized field layout (those vary by service, nation, and
// doctrine, and this package was deliberately not built to hardcode
// one) -- Kind, Fields, and Narrative are whatever the caller decides
// they mean. A SITREP and a Staff Journal entry are both just a Record
// with a different Kind string; nothing in this package treats any kind
// specially.
type Record struct {
	// Kind is the caller-chosen record type name (e.g. "sitrep",
	// "journal", or anything else) -- see BuildKey's doc comment for how
	// it's encoded into the storage key.
	Kind string `json:"kind"`
	// UnitID is the caller-chosen scope a record belongs to (a unit, a
	// vessel, a desk -- whatever the caller's own organizing concept is).
	UnitID string `json:"unit_id"`
	// Timestamp is when the record was logged. Also encoded into the
	// storage key (BuildKey) to make chronological range scans possible;
	// duplicated here so a decoded Record is self-contained without
	// needing its own key parsed back out via ParseKey.
	Timestamp time.Time `json:"timestamp"`
	// AuthorPeerID identifies who logged the record -- typically the
	// libp2p peer id of the node pkg/kvctl.LogAppend was called against.
	AuthorPeerID string `json:"author_peer_id"`
	// Fields is an open key/value bag for whatever structured content
	// the caller's report format needs -- e.g. a LOGSTAT's fuel/ammo
	// figures, a CASREP's casualty counts. Entirely caller-defined.
	Fields map[string]string `json:"fields,omitempty"`
	// Narrative is free-text content -- e.g. a SITREP's situation
	// summary or a journal entry's watch narrative.
	Narrative string `json:"narrative,omitempty"`
}

// Encode marshals r to JSON, the wire/storage form pkg/kvctl.LogAppend
// writes as a record's pkg/store value.
func (r Record) Encode() ([]byte, error) {
	return json.Marshal(r)
}

// Decode unmarshals a Record previously produced by Encode.
func Decode(data []byte) (Record, error) {
	var r Record
	err := json.Unmarshal(data, &r)
	return r, err
}
