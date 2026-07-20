package registry_test

import (
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

func openTestRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	t.Setenv(registry.EnvHome, t.TempDir())
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	return reg
}

// TestDeleteRemovesEntry checks the basic case: an existing node's entry is
// gone from List/Get after Delete.
func TestDeleteRemovesEntry(t *testing.T) {
	reg := openTestRegistry(t)
	info := registry.NodeInfo{PeerID: "peer-a", Role: registry.RoleLeader, DataDir: "/tmp/peer-a"}
	if err := reg.Put(info); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := reg.Delete("peer-a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, err := reg.Get("peer-a"); err != nil {
		t.Fatalf("Get after Delete: %v", err)
	} else if ok {
		t.Fatalf("Get after Delete: still present")
	}
}

// TestDeleteClearsCurrent checks that deleting the node currently selected
// for Set/Get clears that selection too, rather than leaving Current()
// pointing at a peer id with no registry entry.
func TestDeleteClearsCurrent(t *testing.T) {
	reg := openTestRegistry(t)
	if err := reg.Put(registry.NodeInfo{PeerID: "peer-a", DataDir: "/tmp/peer-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := reg.SetCurrent("peer-a"); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	if err := reg.Delete("peer-a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := reg.Current(); err == nil {
		t.Fatalf("Current() after deleting the current node: want error, got none")
	}
}

// TestDeleteLeavesOtherCurrent checks Delete doesn't clear Current when the
// deleted node isn't the one selected.
func TestDeleteLeavesOtherCurrent(t *testing.T) {
	reg := openTestRegistry(t)
	if err := reg.Put(registry.NodeInfo{PeerID: "peer-a", DataDir: "/tmp/peer-a"}); err != nil {
		t.Fatalf("Put(peer-a): %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: "peer-b", DataDir: "/tmp/peer-b"}); err != nil {
		t.Fatalf("Put(peer-b): %v", err)
	}
	if err := reg.SetCurrent("peer-b"); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	if err := reg.Delete("peer-a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	cur, err := reg.Current()
	if err != nil {
		t.Fatalf("Current() after deleting an unrelated node: %v", err)
	}
	if cur != "peer-b" {
		t.Fatalf("Current() = %q, want %q", cur, "peer-b")
	}
}

// TestDeleteUnknownPeerIsNoop checks Delete on a peer id that was never
// registered doesn't error.
func TestDeleteUnknownPeerIsNoop(t *testing.T) {
	reg := openTestRegistry(t)
	if err := reg.Delete("never-existed"); err != nil {
		t.Fatalf("Delete(unknown): %v", err)
	}
}
