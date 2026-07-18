package daemon

import (
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// TestRelayACL exercises relayACL.AllowReserve/AllowConnect directly
// against a real *store.Store: both must deny a peer with no confirmed
// KindPermitPeer record and allow one once that record exists, since
// that's the entire mechanism Config.RequirePermitForRelay relies on to
// make a confirmed permit mean "may join/use the cluster's relay" (see
// that kind's doc comment).
func TestRelayACL(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}

	acl := relayACL{store: st}

	if acl.AllowReserve(id, nil) {
		t.Fatal("AllowReserve succeeded for a peer with no permit record")
	}
	if acl.AllowConnect(id, nil, "") {
		t.Fatal("AllowConnect succeeded for a peer with no permit record")
	}

	key := shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(id.String()))
	if err := st.Set(key, nil); err != nil {
		t.Fatalf("Set confirmed permit: %v", err)
	}

	if !acl.AllowReserve(id, nil) {
		t.Fatal("AllowReserve rejected a peer with a confirmed permit record")
	}
	if !acl.AllowConnect(id, nil, "") {
		t.Fatal("AllowConnect rejected a peer with a confirmed permit record")
	}
}
