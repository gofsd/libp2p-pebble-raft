package kvfsm_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestApplyOpConfirmPromotesPendingRecord(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := kvfsm.New(s)

	pendingKey := []byte{0x00, 0x01, 0x01, 'p', 'e', 'e', 'r'}
	confirmedKey := []byte{0x00, 0x01, 0x02, 'p', 'e', 'e', 'r'}

	setCmd := kvfsm.EncodeCommand(kvfsm.OpSet, pendingKey, []byte("metadata"))
	if res, ok := f.Apply(&raft.Log{Data: setCmd}).(kvfsm.ApplyResult); !ok || res.Err != nil {
		t.Fatalf("Apply OpSet: %+v", res)
	}

	confirmCmd := kvfsm.EncodeCommand(kvfsm.OpConfirm, pendingKey, confirmedKey)
	if res, ok := f.Apply(&raft.Log{Data: confirmCmd}).(kvfsm.ApplyResult); !ok || res.Err != nil {
		t.Fatalf("Apply OpConfirm: %+v", res)
	}

	v, err := s.Get(confirmedKey)
	if err != nil {
		t.Fatalf("Get confirmed key: %v", err)
	}
	if string(v) != "metadata" {
		t.Fatalf("confirmed value = %q, want %q", v, "metadata")
	}

	if _, err := s.Get(pendingKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pending key: got %v, want ErrNotFound (should be deleted after confirm)", err)
	}
}

func TestApplyOpConfirmPromotesEmptyPendingValue(t *testing.T) {
	// Regression test: a pending record with an empty (but valid --
	// EventPermitRequest's metadata is optional) value previously made
	// OpConfirm fail with a NOT NULL constraint error, because
	// pkg/store.Store.Get can return a nil []byte for a stored empty
	// value, and re-Setting that nil value elsewhere bound as SQL NULL
	// against the value column's NOT NULL constraint (see
	// pkg/store.Store.Set's doc comment for the fix).
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := kvfsm.New(s)

	pendingKey := []byte{0x00, 0x01, 0x01, 'p', 'e', 'e', 'r', '2'}
	confirmedKey := []byte{0x00, 0x01, 0x02, 'p', 'e', 'e', 'r', '2'}

	setCmd := kvfsm.EncodeCommand(kvfsm.OpSet, pendingKey, nil)
	if res, ok := f.Apply(&raft.Log{Data: setCmd}).(kvfsm.ApplyResult); !ok || res.Err != nil {
		t.Fatalf("Apply OpSet with empty value: %+v", res)
	}

	confirmCmd := kvfsm.EncodeCommand(kvfsm.OpConfirm, pendingKey, confirmedKey)
	if res, ok := f.Apply(&raft.Log{Data: confirmCmd}).(kvfsm.ApplyResult); !ok || res.Err != nil {
		t.Fatalf("Apply OpConfirm with empty pending value: %+v", res)
	}

	if _, err := s.Get(confirmedKey); err != nil {
		t.Fatalf("Get confirmed key: %v", err)
	}
}

func TestApplyOpConfirmWithNoPendingRecordFails(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := kvfsm.New(s)

	pendingKey := []byte{0x00, 0x01, 0x01, 'g', 'h', 'o', 's', 't'}
	confirmedKey := []byte{0x00, 0x01, 0x02, 'g', 'h', 'o', 's', 't'}

	confirmCmd := kvfsm.EncodeCommand(kvfsm.OpConfirm, pendingKey, confirmedKey)
	res, ok := f.Apply(&raft.Log{Data: confirmCmd}).(kvfsm.ApplyResult)
	if !ok {
		t.Fatalf("Apply did not return kvfsm.ApplyResult")
	}
	if res.Err == nil {
		t.Fatal("Apply OpConfirm with no pending record unexpectedly succeeded")
	}

	if _, err := s.Get(confirmedKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("confirmed key: got %v, want ErrNotFound (nothing should have been written)", err)
	}
}
