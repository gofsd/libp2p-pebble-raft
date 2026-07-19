package logrecord_test

import (
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestKeyRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey("sitrep", "1BCT", ts, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	gotKind, gotUnitID, gotTS, err := logrecord.ParseKey(key)
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if gotKind != "sitrep" {
		t.Fatalf("ParseKey kind = %q, want %q", gotKind, "sitrep")
	}
	if gotUnitID != "1BCT" {
		t.Fatalf("ParseKey unitID = %q, want %q", gotUnitID, "1BCT")
	}
	if !gotTS.Equal(ts) {
		t.Fatalf("ParseKey timestamp = %v, want %v", gotTS, ts)
	}
}

// TestKeyOrderingByteAmbiguity proves BuildKey's length-prefixing keeps
// range scans exact even when one kind/unitID is a byte-prefix of
// another -- without it, "1BCT" and "1BCT2" (or "sitrep"/"sitrep2")
// would produce keys where one is a literal prefix of the other,
// corrupting any range scan bounded to just one of them.
func TestKeyOrderingByteAmbiguity(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	write := func(kind, unitID string, ts time.Time) {
		t.Helper()
		rnd, err := logrecord.NewRand()
		if err != nil {
			t.Fatalf("NewRand: %v", err)
		}
		key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
		if err != nil {
			t.Fatalf("BuildKey(%q, %q): %v", kind, unitID, err)
		}
		if err := s.Set(key, []byte(kind+"/"+unitID)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	write("sitrep", "1BCT", base)
	write("sitrep", "1BCT2", base) // unitID is a byte-prefix extension
	write("sitrep2", "1BCT", base) // kind is a byte-prefix extension
	write("journal", "1BCT", base) // unrelated kind entirely

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	got, err := s.ScanRange(lo, hi, 0)
	if err != nil {
		t.Fatalf("ScanRange: %v", err)
	}
	if len(got) != 1 || string(got[0].Value) != "sitrep/1BCT" {
		t.Fatalf("ScanRange(sitrep, 1BCT) = %+v, want exactly {sitrep/1BCT}", got)
	}

	// KindPrefix("sitrep") must include "sitrep"'s own unit variants but
	// never leak into "sitrep2"'s.
	got, err = s.ScanPrefix(logrecord.KindPrefix("sitrep"), 0)
	if err != nil {
		t.Fatalf("ScanPrefix(KindPrefix): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ScanPrefix(KindPrefix(sitrep)) returned %d entries, want 2 (1BCT and 1BCT2, not sitrep2): %+v", len(got), got)
	}

	// AllPrefix must see every kind.
	got, err = s.ScanPrefix(logrecord.AllPrefix(), 0)
	if err != nil {
		t.Fatalf("ScanPrefix(AllPrefix): %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("ScanPrefix(AllPrefix) returned %d entries, want 4", len(got))
	}
}

func TestScanBoundsTimeWindow(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	times := []time.Time{
		base,
		base.Add(1 * time.Hour),
		base.Add(2 * time.Hour),
		base.Add(3 * time.Hour),
	}
	for i, ts := range times {
		rnd, err := logrecord.NewRand()
		if err != nil {
			t.Fatalf("NewRand: %v", err)
		}
		key, err := logrecord.BuildKey("sitrep", "1BCT", ts, rnd)
		if err != nil {
			t.Fatalf("BuildKey: %v", err)
		}
		if err := s.Set(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Window covering hour 1 through hour 2 inclusive must return exactly
	// those two records, including the boundary at hour 2 itself.
	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", times[1], times[2])
	got, err := s.ScanRange(lo, hi, 0)
	if err != nil {
		t.Fatalf("ScanRange: %v", err)
	}
	if len(got) != 2 || got[0].Value[0] != 1 || got[1].Value[0] != 2 {
		t.Fatalf("ScanRange(hour1, hour2) = %+v, want indices {1, 2}", got)
	}
}
