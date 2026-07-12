package e2edata

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	lp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// readFileHex and peerIDFromMarshaledKey mirror pkg/daemon.loadKey exactly,
// so TestWriteDesktopKeyFileProducesSamePeerID proves WriteDesktopKeyFile
// produces a file byte-for-byte compatible with what a real kvnode
// -key-path load expects, without importing pkg/daemon (which would be a
// cyclic-feeling dependency for a small identity-format check).
func readFileHex(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(string(data))
}

func peerIDFromMarshaledKey(raw []byte) (string, error) {
	priv, err := lp2pcrypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return "", err
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", err
	}
	return pid.String(), nil
}

func TestVersionLifecycle(t *testing.T) {
	f := &File{Versions: map[int]string{}, Nodes: map[int]Node{}}
	if v := f.CurrentVersion(); v != 0 {
		t.Fatalf("CurrentVersion on empty file = %d, want 0", v)
	}

	v1 := f.NewVersion("initial")
	if v1 != 1 {
		t.Fatalf("first NewVersion = %d, want 1", v1)
	}
	if f.CurrentVersion() != 1 {
		t.Fatalf("CurrentVersion = %d, want 1", f.CurrentVersion())
	}

	v2 := f.NewVersion("add learner")
	if v2 != 2 {
		t.Fatalf("second NewVersion = %d, want 2", v2)
	}
	if f.CurrentVersion() != 2 {
		t.Fatalf("CurrentVersion = %d, want 2", f.CurrentVersion())
	}
}

func TestAddNodeAndAddTest(t *testing.T) {
	f := &File{Versions: map[int]string{}, Nodes: map[int]Node{}}

	id1, n1, err := f.AddNode(PlatformDesktop)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if id1 != 1 {
		t.Fatalf("first node id = %d, want 1", id1)
	}
	if n1.PeerID == "" {
		t.Fatal("AddNode: empty peer id")
	}
	priv, err := n1.PrivateKeyBytes()
	if err != nil {
		t.Fatalf("PrivateKeyBytes: %v", err)
	}
	if len(priv) != shmevent.PrivateKeySize {
		t.Fatalf("private key len = %d, want %d", len(priv), shmevent.PrivateKeySize)
	}

	id2, _, err := f.AddNode(PlatformWeb)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if id2 != 2 {
		t.Fatalf("second node id = %d, want 2", id2)
	}

	row, err := f.AddTest(id1, NewEvent(shmevent.EventGetPublicKey, 0, 0, nil, 0))
	if err != nil {
		t.Fatalf("AddTest: %v", err)
	}
	if row.Version != 1 {
		t.Fatalf("AddTest on empty version history created row with version %d, want 1", row.Version)
	}
	if len(f.Rows) != 1 {
		t.Fatalf("len(Rows) = %d, want 1", len(f.Rows))
	}

	if _, err := f.AddTest(999, Event{}); err == nil {
		t.Fatal("AddTest with unknown node id: want error, got nil")
	}
}

func TestPendingRows(t *testing.T) {
	f := &File{Versions: map[int]string{}, Nodes: map[int]Node{}}
	id, _, err := f.AddNode(PlatformDesktop)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	f.NewVersion("v1")
	if _, err := f.AddTest(id, NewEvent(shmevent.EventGetPublicKey, 0, 0, nil, 0)); err != nil {
		t.Fatal(err)
	}
	f.MarkPublished()

	f.NewVersion("v2")
	if _, err := f.AddTest(id, NewEvent(shmevent.EventGetPrivateKey, 0, 0, nil, 0)); err != nil {
		t.Fatal(err)
	}

	pending := f.PendingRows()
	if len(pending) != 1 {
		t.Fatalf("PendingRows after publishing v1 = %d rows, want 1", len(pending))
	}
	if f.Rows[pending[0]].Version != 2 {
		t.Fatalf("pending row version = %d, want 2", f.Rows[pending[0]].Version)
	}

	all := f.AllRowIndices()
	if len(all) != 2 {
		t.Fatalf("AllRowIndices = %d, want 2", len(all))
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testdata.json")

	f := &File{Versions: map[int]string{}, Nodes: map[int]Node{}}
	f.NewVersion("v1")
	id, _, err := f.AddNode(PlatformDesktop)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := f.AddTest(id, NewEvent(shmevent.EventSetField, 5, 0, []byte("world"), 0)); err != nil {
		t.Fatal(err)
	}

	if err := f.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.CurrentVersion() != 1 {
		t.Fatalf("loaded CurrentVersion = %d, want 1", loaded.CurrentVersion())
	}
	gotValue, err := loaded.Rows[0].Event.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if len(loaded.Rows) != 1 || string(gotValue) != "world" {
		t.Fatalf("loaded rows mismatch: %+v", loaded.Rows)
	}
	if loaded.Nodes[id].PeerID != f.Nodes[id].PeerID {
		t.Fatalf("loaded node peer id mismatch")
	}
}

func TestLoadMissingFile(t *testing.T) {
	f, err := Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err != nil {
		t.Fatalf("Load missing file: %v", err)
	}
	if f.CurrentVersion() != 0 || len(f.Nodes) != 0 {
		t.Fatalf("Load missing file: want empty File, got %+v", f)
	}
}

func TestWriteDesktopKeyFileProducesSamePeerID(t *testing.T) {
	pub, priv, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	_ = pub
	n := Node{
		Platform:   PlatformDesktop,
		PrivateKey: encodeHex(priv),
	}
	wantPeerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}

	keyPath := filepath.Join(t.TempDir(), "identity.key")
	if err := WriteDesktopKeyFile(n, keyPath); err != nil {
		t.Fatalf("WriteDesktopKeyFile: %v", err)
	}

	// Mirror pkg/daemon.loadKey exactly: hex-decode, UnmarshalPrivateKey,
	// derive peer id -- confirming the file this writes is byte-for-byte
	// what a real kvnode -key-path load expects.
	data, err := readFileHex(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	gotPeerID, err := peerIDFromMarshaledKey(data)
	if err != nil {
		t.Fatalf("peerIDFromMarshaledKey: %v", err)
	}
	if gotPeerID != wantPeerID {
		t.Fatalf("peer id from written key file = %s, want %s", gotPeerID, wantPeerID)
	}
}
