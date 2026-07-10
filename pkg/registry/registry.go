// Package registry tracks, on the local filesystem, the KV-store nodes that
// have been created on this machine: their peer id, role, data directory,
// and listen addresses. It also tracks which node is "current" for the
// mage set/get commands to target.
//
// It exists because a node is identified everywhere else (shmring channel
// names, raft ServerID, on-disk data directory) by its libp2p peer id, but a
// human operator needs to go from "the leader I just created" to that peer
// id, and a freshly-spawned follower needs to resolve "the leader's peer id"
// to a dialable multiaddr. Both directions are answered by this file.
//
// This package is meant for a single operator driving commands sequentially
// from a CLI, not concurrent writers; it does not implement cross-process
// locking beyond atomic file replacement.
package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Role identifies whether a node was created as the initial cluster leader
// or joined later as a follower. It is informational only: raft itself
// decides and can change leadership at runtime.
type Role string

const (
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
)

// NodeInfo is everything the CLI/daemon need to know about a node without
// having to ask the (possibly not-running) daemon itself.
type NodeInfo struct {
	PeerID       string   `json:"peer_id"`
	Role         Role     `json:"role"`
	DataDir      string   `json:"data_dir"`
	KeyPath      string   `json:"key_path"`
	ListenAddrs  []string `json:"listen_addrs"`
	LeaderPeerID string   `json:"leader_peer_id,omitempty"`
	PID          int      `json:"pid"`
}

// file is the on-disk shape of registry.json.
type file struct {
	Nodes map[string]NodeInfo `json:"nodes"`
}

// Registry is a handle on the on-disk node registry rooted at Dir.
type Registry struct {
	Dir string
}

// EnvHome, when set, overrides the default registry root. Tests use this to
// isolate their state from the operator's real registry.
const EnvHome = "KVSTORE_HOME"

// Open resolves the registry root (EnvHome, or ~/.libp2p-kv-raft),
// creates it if necessary, and returns a handle to it.
func Open() (*Registry, error) {
	dir := os.Getenv(EnvHome)
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("registry: resolve home dir: %w", err)
		}
		dir = filepath.Join(home, ".libp2p-kv-raft")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("registry: create %s: %w", dir, err)
	}
	return &Registry{Dir: dir}, nil
}

func (r *Registry) jsonPath() string    { return filepath.Join(r.Dir, "registry.json") }
func (r *Registry) currentPath() string { return filepath.Join(r.Dir, "current") }

// NodeDataDir returns the directory a node identified by peerID should store
// its identity key, pebble data, and raft log under. Callers use this before
// a NodeInfo even exists in the registry, when provisioning a brand new (or
// resuming an existing) node.
func (r *Registry) NodeDataDir(peerID string) string {
	return filepath.Join(r.Dir, "nodes", peerID)
}

func (r *Registry) load() (file, error) {
	var f file
	data, err := os.ReadFile(r.jsonPath())
	if os.IsNotExist(err) {
		f.Nodes = map[string]NodeInfo{}
		return f, nil
	}
	if err != nil {
		return f, err
	}
	if len(data) == 0 {
		f.Nodes = map[string]NodeInfo{}
		return f, nil
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return f, fmt.Errorf("registry: parse %s: %w", r.jsonPath(), err)
	}
	if f.Nodes == nil {
		f.Nodes = map[string]NodeInfo{}
	}
	return f, nil
}

func (r *Registry) save(f file) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.jsonPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.jsonPath())
}

// Put persists (creating or overwriting) info for info.PeerID.
func (r *Registry) Put(info NodeInfo) error {
	f, err := r.load()
	if err != nil {
		return err
	}
	f.Nodes[info.PeerID] = info
	return r.save(f)
}

// Get returns the registered info for peerID, if any.
func (r *Registry) Get(peerID string) (NodeInfo, bool, error) {
	f, err := r.load()
	if err != nil {
		return NodeInfo{}, false, err
	}
	info, ok := f.Nodes[peerID]
	return info, ok, nil
}

// List returns every registered node.
func (r *Registry) List() ([]NodeInfo, error) {
	f, err := r.load()
	if err != nil {
		return nil, err
	}
	out := make([]NodeInfo, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		out = append(out, n)
	}
	return out, nil
}

// ResolveAddress returns a dialable multiaddr (including /p2p/<peerID>) for
// peerID, looked up from the local registry. It is used to turn a bare peer
// id (what a human, or `mage addnode`, provides) into a raft.ServerAddress.
//
// It only works for nodes created on *this* machine, since the registry is
// a local file. A leader on another machine (e.g. a remote deployment
// joined over SSH) has no shared registry to resolve from -- callers should
// check IsMultiaddr first and, if the caller-supplied string is already a
// full multiaddr, use it directly instead of calling ResolveAddress.
func (r *Registry) ResolveAddress(peerID string) (string, error) {
	info, ok, err := r.Get(peerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("registry: unknown peer id %s (not created on this machine)", peerID)
	}
	if len(info.ListenAddrs) == 0 {
		return "", fmt.Errorf("registry: peer id %s has no known listen address", peerID)
	}
	return info.ListenAddrs[0], nil
}

// IsMultiaddr reports whether s looks like a multiaddr (e.g.
// "/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...") rather than a bare peer id (e.g.
// "12D3Koo..."). Multiaddrs always start with "/"; peer ids never do. Used
// to decide whether a leader identifier can be dialed directly or needs
// resolving through the local registry.
func IsMultiaddr(s string) bool {
	return strings.HasPrefix(s, "/")
}

// Current returns the peer id of the "active" node that set/get target, or
// an error if none has been selected yet.
func (r *Registry) Current() (string, error) {
	data, err := os.ReadFile(r.currentPath())
	if os.IsNotExist(err) {
		return "", fmt.Errorf("registry: no current node selected; run `mage addnode` or `mage use <peer-id>` first")
	}
	if err != nil {
		return "", err
	}
	peerID := string(data)
	return peerID, nil
}

// SetCurrent records peerID as the active node for set/get.
func (r *Registry) SetCurrent(peerID string) error {
	tmp := r.currentPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(peerID), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.currentPath())
}
