package store_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestSetGetDelete(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.Get([]byte("missing")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get missing key: got %v, want ErrNotFound", err)
	}

	if err := s.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, err := s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get: got %q, want %q", v, "v1")
	}

	// Set again should overwrite, not error or duplicate.
	if err := s.Set([]byte("k1"), []byte("v2")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	v, err = s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get after overwrite: %v", err)
	}
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("Get after overwrite: got %q, want %q", v, "v2")
	}

	if err := s.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get([]byte("k1")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestSetNilValue(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Set([]byte("k1"), nil); err != nil {
		t.Fatalf("Set with nil value: %v", err)
	}
	v, err := s.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("Get after Set(nil): got %q, want empty", v)
	}

	// Re-Set that same (possibly nil, per Get's own doc comment) value
	// under a different key -- the exact read-then-write pattern
	// kvfsm's OpConfirm performs, and the one that originally surfaced
	// this as a NOT NULL constraint failure.
	if err := s.Set([]byte("k2"), v); err != nil {
		t.Fatalf("Set(k2, <value read back from k1>): %v", err)
	}
}

func TestDumpAllLoadAllRoundTrip(t *testing.T) {
	src, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	defer src.Close()

	want := map[string]string{
		"a":   "1",
		"bb":  "22",
		"ccc": "333",
		"":    "empty-key",
	}
	for k, v := range want {
		if err := src.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}

	var buf bytes.Buffer
	if err := src.DumpAll(&buf); err != nil {
		t.Fatalf("DumpAll: %v", err)
	}

	dst, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer dst.Close()

	// Pre-populate dst with a key that must not survive LoadAll, mirroring
	// how raft's Restore calls it against a store that may already have
	// (stale) content.
	if err := dst.Set([]byte("stale"), []byte("x")); err != nil {
		t.Fatalf("Set stale: %v", err)
	}

	if err := dst.LoadAll(&buf); err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if _, err := dst.Get([]byte("stale")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale key survived LoadAll: err=%v", err)
	}
	for k, wantV := range want {
		gotV, err := dst.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%q) after LoadAll: %v", k, err)
		}
		if string(gotV) != wantV {
			t.Fatalf("Get(%q) after LoadAll: got %q, want %q", k, gotV, wantV)
		}
	}
}

func TestReopenPersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sqlite")

	s1, err := store.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := store.Open(dir)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close()
	v, err := s2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("Get after reopen: got %q, want %q", v, "v")
	}
}
