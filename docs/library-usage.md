# Using this repo as a library from another Go project

This doc is a task-oriented API reference for writing Go code that imports
`github.com/gofsd/libp2p-kv-raft` from a *different* module — as opposed to
`README.md`, which documents this repo's own architecture and its `mage`
CLI. If you're generating code for a caller project, this file plus reading
the referenced source files' doc comments is enough context; you don't need
the rest of the repo's history to get it right.

**Don't use `docs/getting-started.md`, `docs/linux.md`, or `docs/android.md`**
— they describe an earlier prototype (`pkg/raft.NewP2PNode`, `cmd/client`)
that no longer exists. This file and `README.md` are the only current docs.

## Mental model

There is no client library that dials a node over the network the way a
Postgres or Redis driver would. A **node** is a separate OS process
(`kvnode`, built from `cmd/kvnode`) holding the raft state machine, the
libp2p host, and the SQLite store. Your program talks to *one specific node
process on the same machine* over `pkg/ipc` (shared memory, not a socket) —
`pkg/shmclient` is the Go API for that conversation. So integrating looks
like:

1. Your program spawns (or already has running nearby) a `kvnode` process —
   or embeds the daemon in-process via `pkg/daemon.Run`, the same way
   `mobile/kvmobile` does for Android.
2. Your program calls `pkg/shmclient` functions, giving them that node's
   **peer ID** (a string, not a network address) to identify which local
   node to talk to.
3. `pkg/shmclient` opens shared memory to that peer ID's process, signs the
   request with the node's own Ed25519 key (fetched once via
   `GetPrivateKey` — see "Why the client borrows the node's key" below),
   and gets a response back.

If you only need `set`/`get` from a shell script or another language
entirely, skip the Go API and shell out to `cmd/kvctl-cli` instead — see
["No Go toolchain available"](#no-go-toolchain-available) below.

## Install

```bash
go get github.com/gofsd/libp2p-kv-raft
```

Requires Go 1.25+. No CGO toolchain needed (SQLite is pure-Go
`modernc.org/sqlite`).

## Quickest path: `pkg/kvctl`

`pkg/kvctl` is the highest-level package — it wraps process spawning,
`registry.Registry` bookkeeping (which peer ID is "current", where its data
dir lives), and `pkg/shmclient` calls together, exactly like the `mage
addnode`/`set`/`get` targets do. Prefer this over calling `pkg/shmclient`
directly unless you need to manage multiple concurrently-selected nodes or
already have your own process-spawning logic.

```go
import (
	"fmt"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

func main() {
	// repoRoot must contain this module's source (kvctl.AddNode builds
	// cmd/kvnode from source with `go build`). Use kvctl.AddNodeWithBinary
	// instead if you've already built/cross-compiled a kvnode binary and
	// want to skip the build step (e.g. deploying to a machine with no Go
	// toolchain).
	repoRoot := "/path/to/libp2p-kv-raft"

	// 0 args: bootstrap this new node as the cluster's sole leader.
	peerID, err := kvctl.AddNode(repoRoot)
	if err != nil {
		panic(err)
	}
	fmt.Println("leader peer id:", peerID)

	// AddNode also selects peerID as "current" in the local registry, so
	// Set/Get below need no peer ID argument.
	if err := kvctl.Set("hello", "world"); err != nil {
		panic(err)
	}
	value, err := kvctl.Get("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(value) // "world"
}
```

Joining a second node to that leader (e.g. from a second process, possibly
on another machine reusing the same registry directory conventions):

```go
// 1 arg: create a brand new node and join it to leaderPeerID as a raft
// voter. leaderPeerID may be a bare peer ID (if created on this same
// machine) or a full multiaddr, e.g.
// "/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...", for a leader on another machine.
followerID, err := kvctl.AddNode(repoRoot, leaderPeerID)
```

Restarting a node that already exists in the registry after a crash/reboot:

```go
// Same address as before, no leader coordination needed:
kvctl.ResumeNode(repoRoot, ownPeerID)

// Address changed, or a new/different leader needs to learn about it:
kvctl.AddNode(repoRoot, leaderPeerID, ownPeerID) // 2-arg rejoin form
```

Moving an *existing* node between clusters, rather than spawning a new one — `Join` reuses
`AddNode`'s own join wire protocol under the current identity (`registry.Registry.Current`),
stopping/restarting its process as needed:

```go
// Ask targetPeerID's cluster to admit the current node. Immediate, or
// pending-until-confirmed, depending on targetPeerID's own daemon's
// Config.RequireConfirmForJoin -- see README.md's "Changing which cluster
// a node belongs to" section.
peerID, err := kvctl.Join(repoRoot, targetPeerID)

// If a confirmation is required, run this against any current raft voter
// of targetPeerID's cluster (thin wrapper over ConfirmPermit(KindClusterJoin, ...)):
kvctl.ConfirmPermit(shmevent.KindClusterJoin, []byte(peerID))

// Gracefully leave: raft.RemoveServer (the remaining voters keep operating
// normally), then resume ownPeerID's own solo db. The composite cluster
// dir is left on disk, so a later Join/AddNode-rejoin picks it back up.
err = kvctl.Leave(repoRoot, ownPeerID)

// Leave, plus revoke this identity's cluster-join standing (so a later
// Join needs a fresh confirmation, not a stale one) and delete the
// composite cluster dir outright.
err = kvctl.Rm(repoRoot, ownPeerID)
```

Other `pkg/kvctl` functions worth knowing (all operate on the registry's
"current" node unless noted):

| Function | Purpose |
|---|---|
| `Use(peerID string) error` | Switch which node is "current" for `Set`/`Get`. |
| `GetFrom(peerID, key string) (string, error)` | Read from a specific node without disturbing "current". |
| `LogAppend(kind, unitID string, fields map[string]string, narrative string) error` | Write a `pkg/logrecord.Record` (see below). |
| `LogQuery(kind, unitID string, start, end time.Time, limit int) ([]logrecord.Record, error)` | Scan records in a time window, oldest first. |
| `Execute(destPeerID, value string) error` | Send a raw peer-to-peer notification, bypassing raft entirely. |
| `PollExecute() (senderPeerID, value string, ok bool, err error)` | Drain one queued `Execute` notification. |
| `RequestPermit`/`ConfirmPermit`/`RevokePermit(kind byte, targetPeerID, metadata []byte)` | Manage relay/remote-access/cluster-join permits — see `README.md`'s permit sections for when these are needed. `kind` is `shmevent.KindPermitPeer`/`KindBootstrapNode`/`KindClusterJoin`. |
| `RequestLogPermit`/`ConfirmLogPermit`/`RevokeLogPermit(logKind string, targetPeerID, metadata []byte)` | Same, scoped per `pkg/logrecord` kind. |
| `Join(repoRoot, targetPeerID string) (string, error)` | Switch the current identity onto targetPeerID's cluster (see above). |
| `Leave(repoRoot, ownPeerID string) error` | Gracefully leave ownPeerID's current cluster; resumes its solo db. |
| `Rm(repoRoot, ownPeerID string) error` | `Leave` plus revoke standing and delete the composite cluster dir. |

All of `kvctl`'s registry-backed functions (`Set`, `Get`, `Use`, the permit
helpers, `LogAppend`/`LogQuery`) read `registry.Open()`'s on-disk state —
`~/.libp2p-kv-raft` by default, or `$KVSTORE_HOME` if set. Only one process
should call `registry.Open()` against a given directory concurrently for
mutating calls (`Put`/`SetCurrent`); reads are safe to interleave.

## Talking to a specific node directly: `pkg/shmclient`

Use this instead of `pkg/kvctl` when you already know a node's peer ID (no
registry lookup needed) and want to issue calls against it directly — e.g.
your program manages several node peer IDs itself, or you're embedding the
daemon in-process (see below) and don't want `pkg/kvctl`'s subprocess/build
assumptions at all.

```go
import (
	"context"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

ctx := context.Background()

// One-shot convenience functions (each internally does an Open + call):
err := shmclient.Set(ctx, peerID, "key", "value")
value, err := shmclient.Get(ctx, peerID, "key")
newPeerID, err := shmclient.Add(ctx, peerID, leaderPeerID) // "" leaderPeerID = bootstrap as leader

// A Session amortizes the signing-key fetch across repeated calls — use
// this in a long-lived process issuing many requests to the same node
// (this is what mobile/kvmobile does).
sess, err := shmclient.Open(ctx, peerID)
if err != nil {
	panic(err)
}
err = sess.Set(ctx, "key", "value")
value, err = sess.Get(ctx, "key")
```

Full `Session` method set — see `pkg/shmclient/client.go`'s doc comments
for exact semantics of each:

- `Set(ctx, key, value string) error`
- `Get(ctx, key string) (string, error)`
- `Add(ctx, leaderPeerID string) (string, error)` — bootstrap/join
- `Leave(ctx) error` — ask the current cluster to `raft.RemoveServer` this node (see `README.md`'s `join`/`leave`/`rm` section); local-only, no remote caller may issue it
- `LogAppend(ctx, key, value []byte) error` — raw `pkg/logrecord` write; prefer `pkg/logrecord.BuildKey`+`Record.Encode` to build `key`/`value`, or use `kvctl.LogAppend` for the friendly form
- `ListRange(ctx, start, end []byte) (key, value []byte, ok bool, err error)` — one match at a time; loop, narrowing `start` past the last returned key, for a full range scan
- `RequestPermit`/`ConfirmPermit`/`RevokePermit(ctx, kind byte, peerID, metadata []byte) error`
- `RequestLogPermit`/`ConfirmLogPermit`/`RevokeLogPermit(ctx, logKind string, peerID, metadata []byte) error`
- `Execute(ctx, destPeerID string, payload []byte) error` — direct unreplicated peer-to-peer send, not stored anywhere
- `PollExecute(ctx) (senderPeerID string, payload []byte, ok bool, err error)`

Package-level `GetPublicKey`/`GetPrivateKey(ctx, peerID)` fetch a node's
Ed25519 keys directly (used internally by `Open`; call them yourself only
if you need the raw key material for something else).

### Constraints worth knowing before generating calling code

- **512-byte payload budget.** `shmevent.ValueSize = 512`. `Set`/`LogAppend`
  pack key+value into one message sharing that budget together
  (`shmevent.EncodeSetPayload`); a long key leaves less room for the value.
  Calls exceeding it return an error, not a truncated write.
- **Reserved key namespace.** Keys starting with `shmevent.SystemKeyPrefix`
  (`0x00`) or `logrecord.LogKeyPrefix` are reserved for internal
  permit/membership/log-record bookkeeping — an ordinary `Set` with a
  colliding key is rejected. Don't construct raw keys starting with those
  prefixes yourself; use plain application key strings for `Set`, and
  `logrecord.BuildKey` (never a hand-built key) for `LogAppend`.
- **Errors surface as Go `error`, already unwrapped from the wire.** Every
  `shmclient`/`kvctl` call that can fail returns a plain `error` whose
  message is the daemon's own diagnostic (`pkg/shmevent.EventError`'s
  payload) wrapped with a `"shmclient: <op>: "` / `"kvctl: <op>: "` prefix
  — no custom error types or sentinel values to match against today.
- **Reads can lag writes.** `Get`/`ListRange` read local, possibly-follower
  state — a `Get` immediately after a `Set` you just issued elsewhere may
  not yet reflect it (ordinary raft eventual-consistency-of-followers
  caveat). A `Get` against the same session right after that session's own
  `Set` is fine in practice for the desktop CLI's use, but isn't a
  linearizability guarantee.
- **This only works same-machine.** `pkg/ipc`/`pkg/shmclient` require the
  caller and the `kvnode` process to be on the same host (shared memory).
  A caller on a different machine needs its own local node (join as a
  follower) — there is no remote RPC form of `pkg/shmclient` itself. (The
  one exception, `pkg/daemon.ClientProtocolID`, is a *network* protocol,
  but it's currently only implemented by the Rust `web-app/` client, not
  exposed as a Go package — see `web-app/README.md` if you need a
  cross-machine Go caller and are open to reimplementing that protocol.)

### Why the client borrows the node's key

Every `pkg/shmevent` message (except the two key-bootstrap events
themselves) must carry an Ed25519 signature the daemon checks. A same-
machine caller with `pkg/ipc` access is, by this codebase's threat model,
already as trusted as the node process itself — so rather than mint a
separate identity, `shmclient.Open`/`GetPrivateKey` fetches the node's own
signing key and signs as it. This is *not* how a remote caller
authenticates (see `pkg/daemon`'s `callerIdentity`/`remoteCaller` — a
network caller like `web-app/` signs with its own separate key instead).
Don't assume `GetPrivateKey` generalizes to a network boundary.

## Records: `pkg/logrecord`

For structured, append-only records (journals, situation reports, etc.) on
top of the same raft-replicated path `Set` uses. `kind` is any string you
choose at call time, not a fixed enum:

```go
import (
	"time"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

rnd, _ := logrecord.NewRand()
key, _ := logrecord.BuildKey("sitrep", "1BCT", time.Now(), rnd)
rec := logrecord.Record{
	Kind:      "sitrep",
	UnitID:    "1BCT",
	Timestamp: time.Now(),
	Fields:    map[string]string{"posture": "green"},
	Narrative: "no significant activity",
}
value, _ := rec.Encode()
err := shmclient.LogAppend(ctx, peerID, key, value) // or kvctl.LogAppend(kind, unitID, fields, narrative)
```

Querying a time range for one `(kind, unitID)`, oldest first — prefer
`kvctl.LogQuery`, which already drives the `ListRange` loop for you:

```go
records, err := kvctl.LogQuery("sitrep", "1BCT", start, end, 10 /* limit, 0 = unlimited */)
```

See the JSON payload size caveat above — a record's `Fields`+`Narrative`
share roughly 400-470 bytes after `Kind`/`UnitID`/timestamp overhead, with
no built-in truncation or rotation.

## Embedding the daemon in-process (no subprocess)

If your project can't or shouldn't shell out to a separate `kvnode`
binary — e.g. you're building something like `mobile/kvmobile` — call
`pkg/daemon.Run` directly instead of `pkg/kvctl`'s spawn-a-process path:

```go
import (
	"context"
	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
)

cfg := daemon.Config{
	DataDir: dataDir,
	// KeyPath must point at an existing libp2p identity key file (hex-encoded
	// crypto.MarshalPrivateKey output). Neither pkg/kvctl's nor
	// mobile/kvmobile's key-generation helper is exported -- write your own
	// "load if present, else crypto.GenerateEd25519Key + MarshalPrivateKey +
	// hex-encode to this path" step; mobile/kvmobile/kvmobile.go's
	// ensureIdentity is a complete worked example to copy from.
	KeyPath: keyPath,
	// RelayPeer: relayMultiaddr, // set this if this process has no directly-dialable address (NAT'd/cellular) — see README's "Relay reservations for NAT'd followers"
}
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
errC := make(chan error, 1)
go func() { errC <- daemon.Run(ctx, cfg) }()
```

`daemon.Run` blocks serving IPC until the process exits or the context it's
given is cancelled; read `daemon.ReadReadyFile(dataDir)` (poll it, same as
`pkg/kvctl.waitForReady` does) to learn the node's peer ID and listen
addresses once it's up, then drive it exactly like a subprocess-spawned
node via `pkg/shmclient` — the local IPC transport is the same either way.
This path needs its own care around `Config`'s full field set (raft
timeouts, relay flags, permit-enforcement flags) — read `pkg/daemon.Config`
's doc comments in `pkg/daemon/daemon.go` before using it for anything
beyond a follower, and see `mobile/kvmobile/kvmobile.go` for a complete
real embedding.

## No Go toolchain available

If the calling project isn't Go at all (or you want to avoid vendoring this
module), drive `cmd/kvctl-cli` as a subprocess instead — it needs no Go
toolchain on the machine it runs on, only an already-built `kvnode` binary
next to it:

```bash
kvctl-cli addnode -bin ./kvnode -listen-port 4001   # bootstrap leader, prints its peer id
kvctl-cli set mykey myvalue
kvctl-cli get mykey
```

Run `kvctl-cli -h` (or read `cmd/kvctl-cli/main.go`) for the full command
set — it mirrors `pkg/kvctl`'s exported functions one-to-one (`addnode`,
`resumenode`, `use`, `set`, `get`, `logappend`, `logquery`, `execute`,
`pollexecute`, `requestpermit`/`confirmpermit`/`revokepermit`, the
`logpermit` equivalents, and `sendevent` for raw `pkg/shmevent` dispatch).
`KVSTORE_HOME` controls where its registry/node data lives, same as the Go
API.

## Full example: two-node cluster from one Go program

```go
package main

import (
	"fmt"
	"log"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

func main() {
	repoRoot := "/path/to/libp2p-kv-raft"

	leaderID, err := kvctl.AddNode(repoRoot)
	if err != nil {
		log.Fatalf("bootstrap leader: %v", err)
	}
	fmt.Println("leader:", leaderID)

	followerID, err := kvctl.AddNode(repoRoot, leaderID)
	if err != nil {
		log.Fatalf("join follower: %v", err)
	}
	fmt.Println("follower:", followerID)

	// kvctl.AddNode leaves the just-created node "current" -- switch back
	// to the leader to write, since only the leader (or a node that can
	// forward to it) accepts writes.
	if err := kvctl.Use(leaderID); err != nil {
		log.Fatal(err)
	}
	if err := kvctl.Set("k1", "v1"); err != nil {
		log.Fatalf("set: %v", err)
	}

	// Read back from the follower directly, without disturbing "current".
	value, err := kvctl.GetFrom(followerID, "k1")
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Println("follower read:", value) // "v1" (once replication catches up)
}
```

## Where to look next

- `README.md` — cluster topology, relay/NAT handling, permits, log
  records, the e2e pipeline. Read this for *why* things are shaped this
  way, not just *how* to call them.
- `api/shmevent.capnp` — the wire struct every call above compiles down to;
  its doc comment is the canonical design rationale for the whole
  SetKey/SetField/GetField/Add protocol.
- `pkg/shmclient/client.go`, `pkg/kvctl/kvctl.go` — every function
  mentioned above has a doc comment with more detail than this file
  repeats.
- `CLAUDE.md`'s "Node connectivity policy" section — a real, still-open gap
  in the forwarded-`Set` relay path, relevant if you're deploying a
  multi-node cluster rather than just embedding a single node.
