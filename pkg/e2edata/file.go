// Package e2edata implements the single JSON file that records everything
// the end-to-end test/deploy pipeline needs: a version history, the
// deterministic node identities used across platforms (desktop, android,
// web), and a durable log of test rows (one shmevent, one node, one
// recorded pass/fail) run against those identities.
//
// The file is meant to be committed to the repo (test/e2e/testdata.json):
// identities are deterministic (see identity.go) so every checkout/deploy
// reproduces the exact same peer ids and keys ("predictable deploy" from
// the design brief), and PublishedVersion tracks how far the recorded test
// history has been confirmed passing, so mage e2e:current only re-runs
// what's new since the last time this file's version count moved
// ("version increment based on new tests").
package e2edata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Platform identifies which client implementation a Node's identity is
// deployed under -- each has its own way of being handed a deterministic
// seed (see cmd/kvctl-cli's -key-path file for desktop, mobile/kvmobile's
// identitySeedHex ldflag for android, web-app's worker_main_with_seed for
// web).
type Platform string

const (
	PlatformDesktop Platform = "desktop"
	PlatformAndroid Platform = "android"
	PlatformWeb     Platform = "web"
)

// Node is one deterministic identity recorded in the file, keyed by a small
// integer id in File.Nodes. PublicKey/PrivateKey are hex-encoded stdlib
// crypto/ed25519 keys (32 and 64 raw bytes respectively) -- the same format
// pkg/shmevent.PublicKey/PrivateKey and EventGetPublicKey/EventGetPrivateKey
// use, so a row's expectations can be checked against a live node's actual
// keys with no conversion.
type Node struct {
	Platform   Platform `json:"platform"`
	PeerID     string   `json:"peer_id"`
	PublicKey  string   `json:"public_key"`
	PrivateKey string   `json:"private_key"`
}

// Event is the JSON form of pkg/shmevent.Msg used inside a test Row's
// "event" field and as kvctl-cli sendevent's argument/output shape. Value
// is hex-encoded, not a plain JSON string: EventGetPublicKey/
// EventGetPrivateKey responses (and any tamper/corruption test that
// deliberately sends garbage) carry raw binary that isn't valid UTF-8, and
// encoding/json silently replaces invalid UTF-8 bytes with U+FFFD when
// marshaling a Go string -- a real data-corrupting bug caught by actually
// running sendevent against a live GetPublicKey response before this type
// existed anywhere else, not a hypothetical. Plain KV text (the common
// case) just costs callers a ToValueHex/FromHex round trip.
type Event struct {
	EventType     uint8  `json:"event"`
	SourceID      uint16 `json:"source_id,omitempty"`
	DestinationID uint16 `json:"destination_id,omitempty"`
	ValueHex      string `json:"value_hex,omitempty"`
	ID            uint16 `json:"id,omitempty"`
}

// NewEvent builds an Event carrying value as its (hex-encoded) Value.
func NewEvent(eventType uint8, sourceID, destinationID uint16, value []byte, id uint16) Event {
	return Event{
		EventType:     eventType,
		SourceID:      sourceID,
		DestinationID: destinationID,
		ValueHex:      encodeHex(value),
		ID:            id,
	}
}

// Value decodes e's hex-encoded ValueHex back to raw bytes.
func (e Event) Value() ([]byte, error) {
	if e.ValueHex == "" {
		return nil, nil
	}
	v, err := decodeHex(e.ValueHex)
	if err != nil {
		return nil, fmt.Errorf("e2edata: decode value_hex: %w", err)
	}
	return v, nil
}

// ToMsg converts e to the wire struct pkg/ipc.Call/pkg/shmevent.Encode need.
func (e Event) ToMsg() (shmevent.Msg, error) {
	v, err := e.Value()
	if err != nil {
		return shmevent.Msg{}, err
	}
	return shmevent.Msg{
		EventType:     e.EventType,
		SourceID:      e.SourceID,
		DestinationID: e.DestinationID,
		Value:         v,
		ID:            e.ID,
	}, nil
}

// EventFromMsg converts a decoded response back to the JSON-friendly Event
// shape, for recording/printing.
func EventFromMsg(m shmevent.Msg) Event {
	return NewEvent(m.EventType, m.SourceID, m.DestinationID, m.Value, m.ID)
}

// StatusPass/StatusFail/StatusSkipped are the Row.Status conventions this
// package's runner uses. Any other non-zero value is still "failed" as far
// as File methods are concerned; StatusSkipped exists only so a platform
// this pipeline can't yet drive for real (see e2erun's android gap) doesn't
// get reported as a false failure or a false pass.
const (
	StatusPass    = 0
	StatusFail    = 1
	StatusSkipped = 2
)

// Row is one recorded test: send Event to the node identified by Node
// (against File.Nodes), as it stood the last time this version's tests
// ran, with the outcome in Status/Error.
type Row struct {
	Version int    `json:"version"`
	Node    int    `json:"node"`
	Event   Event  `json:"event"`
	Status  int    `json:"status"`
	Error   string `json:"error,omitempty"`
}

// File is the on-disk shape of test/e2e/testdata.json.
type File struct {
	// Versions maps a version id to a short human label, in the order
	// they were created (ascending keys). CurrentVersion is always the
	// highest key.
	Versions map[int]string `json:"versions"`
	// PublishedVersion is the highest version id confirmed passing and
	// pushed. e2e:current runs every row with Version > PublishedVersion;
	// once they all pass, the pipeline advances PublishedVersion to
	// CurrentVersion.
	PublishedVersion int `json:"published_version"`
	// BootstrapNode is the id (into Nodes) of the desktop identity
	// deployed as the shared SSH bootstrap/leader node, or 0 if none has
	// been provisioned yet (node ids are assigned starting at 1, so 0 is
	// a safe "unset" sentinel).
	BootstrapNode int          `json:"bootstrap_node"`
	Nodes         map[int]Node `json:"nodes"`
	Rows          []Row        `json:"rows"`
}

// DefaultPath is where the testdata file lives relative to the repo root.
const DefaultPath = "test/e2e/testdata.json"

// Load reads and parses the file at path. A missing file is not an error --
// it returns a freshly initialized empty File, since the very first
// `mage e2e:newversion` run has nothing to load yet.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &File{Versions: map[int]string{}, Nodes: map[int]Node{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("e2edata: read %s: %w", path, err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("e2edata: parse %s: %w", path, err)
	}
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	return &f, nil
}

// Save writes f to path, creating parent directories as needed, formatted
// for a readable diff in version control.
func (f *File) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("e2edata: create %s: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("e2edata: encode: %w", err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// CurrentVersion returns the highest version id in Versions -- the
// not-yet-published version any new AddTest row targets -- or 0 if none
// exists yet.
func (f *File) CurrentVersion() int {
	max := 0
	for v := range f.Versions {
		if v > max {
			max = v
		}
	}
	return max
}

// NewVersion records a new version labeled label and returns its id
// (CurrentVersion()+1). Called by `mage e2e:newversion`, or lazily by
// AddTest/AddNode if no version exists yet.
func (f *File) NewVersion(label string) int {
	id := f.CurrentVersion() + 1
	if f.Versions == nil {
		f.Versions = map[int]string{}
	}
	f.Versions[id] = label
	return id
}

// nextNodeID returns the smallest unused key in Nodes greater than 0.
func (f *File) nextNodeID() int {
	max := 0
	for id := range f.Nodes {
		if id > max {
			max = id
		}
	}
	return max + 1
}

// AddNode generates a fresh deterministic identity for platform, records it
// under a new node id, and returns that id.
func (f *File) AddNode(platform Platform) (int, Node, error) {
	pub, priv, err := GenerateIdentity()
	if err != nil {
		return 0, Node{}, err
	}
	peerID, err := PeerIDFromPrivateKey(priv)
	if err != nil {
		return 0, Node{}, err
	}
	if f.Nodes == nil {
		f.Nodes = map[int]Node{}
	}
	id := f.nextNodeID()
	n := Node{
		Platform:   platform,
		PeerID:     peerID,
		PublicKey:  encodeHex(pub),
		PrivateKey: encodeHex(priv),
	}
	f.Nodes[id] = n
	return id, n, nil
}

// AddTest appends a row against CurrentVersion (creating version 1 first if
// none exists yet), targeting nodeID with event ev.
func (f *File) AddTest(nodeID int, ev Event) (Row, error) {
	if _, ok := f.Nodes[nodeID]; !ok {
		return Row{}, fmt.Errorf("e2edata: unknown node id %d", nodeID)
	}
	v := f.CurrentVersion()
	if v == 0 {
		v = f.NewVersion("initial")
	}
	row := Row{Version: v, Node: nodeID, Event: ev}
	f.Rows = append(f.Rows, row)
	return row, nil
}

// PendingRows returns every row whose Version is newer than
// PublishedVersion -- what `mage e2e:current` runs.
func (f *File) PendingRows() []int {
	var idx []int
	for i, r := range f.Rows {
		if r.Version > f.PublishedVersion {
			idx = append(idx, i)
		}
	}
	return idx
}

// AllRowIndices returns every row index -- what `mage e2e:all` runs.
func (f *File) AllRowIndices() []int {
	idx := make([]int, len(f.Rows))
	for i := range f.Rows {
		idx[i] = i
	}
	return idx
}

// MarkPublished advances PublishedVersion to CurrentVersion. The e2e
// runner calls this once every pending row has Status == StatusPass.
func (f *File) MarkPublished() {
	f.PublishedVersion = f.CurrentVersion()
}
