package kvmobile

import (
	"encoding/hex"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// TestEnsureIdentityWithSeed proves a device built with identitySeedHex set
// (the gomobile ldflags injection point, see that var's doc comment) comes
// up as exactly the peer id pkg/e2edata computes for the same raw key --
// the property the e2e pipeline's deterministic-identity design depends on.
func TestEnsureIdentityWithSeed(t *testing.T) {
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	wantPeerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}

	old := identitySeedHex
	identitySeedHex = hex.EncodeToString(priv)
	defer func() { identitySeedHex = old }()

	_, gotPeerID, err := ensureIdentity(t.TempDir())
	if err != nil {
		t.Fatalf("ensureIdentity: %v", err)
	}
	if gotPeerID != wantPeerID {
		t.Fatalf("ensureIdentity with seed = %s, want %s", gotPeerID, wantPeerID)
	}
}

// TestEnsureIdentityReloadsExistingKey confirms a second ensureIdentity
// call against the same dataDir returns the same peer id from the
// persisted key file, regardless of identitySeedHex (which only applies on
// first run, when no key file exists yet).
func TestEnsureIdentityReloadsExistingKey(t *testing.T) {
	dir := t.TempDir()
	_, firstPeerID, err := ensureIdentity(dir)
	if err != nil {
		t.Fatalf("ensureIdentity (first): %v", err)
	}

	old := identitySeedHex
	identitySeedHex = "deadbeef"
	defer func() { identitySeedHex = old }()

	_, secondPeerID, err := ensureIdentity(dir)
	if err != nil {
		t.Fatalf("ensureIdentity (second): %v", err)
	}
	if secondPeerID != firstPeerID {
		t.Fatalf("second ensureIdentity = %s, want %s (existing key file should win over identitySeedHex)", secondPeerID, firstPeerID)
	}
}
