package e2edata

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	lp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// GenerateIdentity creates a fresh stdlib crypto/ed25519 keypair -- the same
// raw format pkg/shmevent.PublicKey/PrivateKey and every platform's
// EventGetPublicKey/EventGetPrivateKey response already use, chosen so a
// node's real, running keys can be compared against what's recorded here
// with no conversion.
func GenerateIdentity() (pub shmevent.PublicKey, priv shmevent.PrivateKey, err error) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("e2edata: generate ed25519 key: %w", err)
	}
	return shmevent.PublicKey(pubKey), shmevent.PrivateKey(privKey), nil
}

// PeerIDFromPrivateKey derives the libp2p peer id a node started with priv
// (in loadKey's -key-path file, see WriteDesktopKeyFile) will report.
func PeerIDFromPrivateKey(priv shmevent.PrivateKey) (string, error) {
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("e2edata: unmarshal ed25519 private key: %w", err)
	}
	pid, err := peer.IDFromPrivateKey(lp2pPriv)
	if err != nil {
		return "", fmt.Errorf("e2edata: derive peer id: %w", err)
	}
	return pid.String(), nil
}

func encodeHex(b []byte) string { return hex.EncodeToString(b) }

func decodeHex(s string) ([]byte, error) { return hex.DecodeString(s) }

// PrivateKeyBytes decodes n.PrivateKey back to raw stdlib ed25519 bytes.
func (n Node) PrivateKeyBytes() (shmevent.PrivateKey, error) {
	raw, err := decodeHex(n.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("e2edata: decode private key for %s: %w", n.PeerID, err)
	}
	if len(raw) != shmevent.PrivateKeySize {
		return nil, fmt.Errorf("e2edata: private key for %s: expected %d bytes, got %d", n.PeerID, shmevent.PrivateKeySize, len(raw))
	}
	return shmevent.PrivateKey(raw), nil
}

// PublicKeyBytes decodes n.PublicKey back to raw stdlib ed25519 bytes.
func (n Node) PublicKeyBytes() (shmevent.PublicKey, error) {
	raw, err := decodeHex(n.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("e2edata: decode public key for %s: %w", n.PeerID, err)
	}
	return shmevent.PublicKey(raw), nil
}

// WriteDesktopKeyFile writes n's private key to keyPath in the exact
// hex-encoded-marshaled-libp2p-key format pkg/daemon's loadKey (and
// pkg/kvctl's generateIdentity, and mobile/kvmobile's ensureIdentity)
// expect at startup -- so a desktop kvnode started with -key-path keyPath
// comes up as exactly the peer id recorded in n.PeerID, with no daemon code
// changes needed.
func WriteDesktopKeyFile(n Node, keyPath string) error {
	raw, err := n.PrivateKeyBytes()
	if err != nil {
		return err
	}
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(raw)
	if err != nil {
		return fmt.Errorf("e2edata: unmarshal ed25519 private key: %w", err)
	}
	marshaled, err := lp2pcrypto.MarshalPrivateKey(lp2pPriv)
	if err != nil {
		return fmt.Errorf("e2edata: marshal libp2p key: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(marshaled)), 0o600); err != nil {
		return fmt.Errorf("e2edata: write key file %s: %w", keyPath, err)
	}
	return nil
}
