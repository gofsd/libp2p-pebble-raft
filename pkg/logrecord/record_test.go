package logrecord_test

import (
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

func TestRecordEncodeDecodeRoundTrip(t *testing.T) {
	want := logrecord.Record{
		Kind:         "sitrep",
		UnitID:       "1BCT",
		Timestamp:    time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
		AuthorPeerID: "12D3KooWExample",
		Fields:       map[string]string{"posture": "green"},
		Narrative:    "no significant activity",
	}
	data, err := want.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := logrecord.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Kind != want.Kind || got.UnitID != want.UnitID || got.AuthorPeerID != want.AuthorPeerID || got.Narrative != want.Narrative {
		t.Fatalf("Decode = %+v, want %+v", got, want)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("Decode timestamp = %v, want %v", got.Timestamp, want.Timestamp)
	}
	if got.Fields["posture"] != "green" {
		t.Fatalf("Decode fields = %+v, want posture=green", got.Fields)
	}
}

func TestRecordEncodeDecodeEmptyFields(t *testing.T) {
	want := logrecord.Record{Kind: "journal", UnitID: "HQ", Timestamp: time.Now()}
	data, err := want.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := logrecord.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Fields) != 0 || got.Narrative != "" {
		t.Fatalf("Decode with no fields/narrative = %+v, want both empty", got)
	}
}
