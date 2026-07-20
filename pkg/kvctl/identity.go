package kvctl

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: marshal key: %w", err)
	}
	return persistIdentity(dataDirFor, priv, raw)
}

// importIdentity loads an existing Ed25519 identity from srcKeyPath -- a
// file in the same hex-encoded format identity.key itself uses, e.g. one
// copied over from another machine or saved from a previous node -- derives
// its peer id, and persists a copy under reg's data directory for that peer
// id, same as generateIdentity does for a freshly minted key. This lets a
// node be (re)provisioned under a specific, already-known identity instead
// of always minting a new one.
func importIdentity(dataDirFor func(peerID string) string, srcKeyPath string) (peerID, dataDir, keyPath string, err error) {
	hexData, err := os.ReadFile(srcKeyPath)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: read key file %s: %w", srcKeyPath, err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(hexData)))
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: decode key file %s: %w", srcKeyPath, err)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: unmarshal key file %s: %w", srcKeyPath, err)
	}
	return persistIdentity(dataDirFor, priv, raw)
}

// persistIdentity derives priv's peer id, creates its data directory, and
// writes raw (priv, hex encoded) to identity.key inside it -- the shared
// tail end of generateIdentity and importIdentity.
func persistIdentity(dataDirFor func(peerID string) string, priv crypto.PrivKey, raw []byte) (peerID, dataDir, keyPath string, err error) {
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", "", "", fmt.Errorf("kvctl: derive peer id: %w", err)
	}
	peerID = pid.String()
	dataDir = dataDirFor(peerID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", "", fmt.Errorf("kvctl: create data dir: %w", err)
	}
	keyPath = filepath.Join(dataDir, "identity.key")
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return "", "", "", fmt.Errorf("kvctl: write key file: %w", err)
	}
	return peerID, dataDir, keyPath, nil
}
