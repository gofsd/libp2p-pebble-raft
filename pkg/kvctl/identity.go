package kvctl

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// generateIdentity creates a new Ed25519 libp2p identity, persists it (hex
// encoded, matching pkg/daemon's loadKey) under reg's data directory for the
// resulting peer id, and returns that peer id, its data dir, and key path.
func generateIdentity(dataDirFor func(peerID string) string) (peerID, dataDir, keyPath string, err error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: generate key pair: %w", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: derive peer id: %w", err)
	}
	peerID = pid.String()
	dataDir = dataDirFor(peerID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", "", fmt.Errorf("kvctl: create data dir: %w", err)
	}

	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: marshal key: %w", err)
	}
	keyPath = filepath.Join(dataDir, "identity.key")
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return "", "", "", fmt.Errorf("kvctl: write key file: %w", err)
	}
	return peerID, dataDir, keyPath, nil
}
