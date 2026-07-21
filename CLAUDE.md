# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A distributed key-value store: [hashicorp/raft](https://github.com/hashicorp/raft) consensus
running over [libp2p](https://github.com/libp2p/go-libp2p) transport, with SQLite (pure-Go
`modernc.org/sqlite`, no CGO) as the on-disk store for the replicated state machine. Nodes run on
separate machines (including behind NAT/cellular, e.g. an Android phone) and are driven locally
over `github.com/gofsd/shmring` shared-memory IPC rather than a network-facing RPC port.

`README.md` is the authoritative architecture doc and is kept current in detail (protocol design,
relay/NAT handling, e2e pipeline). Read it before making non-trivial changes — this file only
summarizes what's needed to get oriented and productive.

## Commands

Requires [mage](https://magefile.org/) on `PATH`.

```bash
mage test          # unit tests: go test -v -short ./...
mage integration    # go test -v -tags=integration ./...
mage testall        # test + integration + every e2e:all row
mage build          # cross-platform build (see Build/BuildLinux/BuildWindows/BuildAndroid in magefile.go)
```

Run a single package/test the normal Go way, e.g. `go test ./pkg/daemon/... -run TestJoinThroughRelay -v`.

Desktop cluster convenience targets (wrap `pkg/kvctl`, see README's "Running a cluster" for the
full walkthrough):

```bash
mage addnode                                    # bootstrap a new leader
mage addfollower "<multiaddr>"                  # join an existing leader
mage addnodewithkey <keyFile>                    # bootstrap a new leader under an existing identity
mage addfollowerwithkey <keyFile> "<multiaddr>"  # join an existing leader under an existing identity
mage resumenode <peerID>                        # restart a node from persisted raft state
mage rejoinnode <leaderAddr> <peerID>            # restart + re-send the join request
mage use <peerID>                                # select the active node for set/get
mage deletenode <peerID>                         # permanently delete a (stopped) node's data + registry entry
mage listclusters                                # show every raft cluster known to this machine's registry
mage listnodes <peerID>                          # query a running node for its cluster's full live peer id list
mage set <key> <value>
mage get <key>
mage rangescan <start> <end> [limit|""]           # list every key/value pair in [start, end] on the current node
```

`listclusters` is a pure local registry read (grouped by whichever peer id originally bootstrapped each
cluster) -- no daemon needs to be running, but for the same reason it only ever shows clusters this
machine has itself created or joined a node into, never a network-wide view. `listnodes <peerID>`, by
contrast, needs `peerID` to actually be running: it queries that node's own locally-replicated
`shmevent.KindClusterMember` records for its raft cluster's current live membership (every voter/
learner/leader, including peers this machine never created and so has no registry entry for at all).

`rangescan <start> <end> [limit]` is the generic counterpart to `set`/`get`: it lists every key/value
pair in `[start, end]` (both inclusive, lexicographic byte order over the raw key bytes) on the
current node, not just one key at a time -- covering ordinary data as well as this project's own
reserved namespaces (`shmevent.SystemKeyPrefix`, `pkg/logrecord`'s prefix), since a local caller
already has unrestricted read access to its own node's entire store the same way `set`/`get` do
(see `pkg/shmevent`'s doc comment on the shmring same-machine trust boundary) -- this isn't a new
privilege, just a convenient way to exercise one that already existed via a raw `sendevent` call.

Cluster-membership lifecycle targets (wrap `pkg/kvctl/cluster.go`; let the *current* node -- `mage use` --
change which cluster it belongs to, rather than spawning a new identity the way `addnode`/`addfollower`
do):

```bash
mage join <targetPeerID>   # ask targetPeerID's cluster to admit the current node's own identity
mage leave <peerID>        # gracefully RemoveServer out of peerID's cluster; resumes its own solo db
mage rm <peerID>           # leave + revoke cluster-join standing + delete the composite cluster data dir
```

`join` reuses `addfollower`'s/`rejoinnode`'s existing wire protocol as-is: whether it's admitted
immediately or first requires a separate confirmation from a raft voter depends entirely on the
target daemon's own `-require-confirm-for-join` setting (`Config.RequireConfirmForJoin`) -- when
set, a join request only lodges a pending `shmevent.KindClusterJoin` record, and `mage
confirmpermit cluster-join <peerID>`, run on any current raft voter (including the leader), is what
actually admits it (`raft.AddVoter`/`AddNonvoter`). `leave`/`rm` shrink the remote cluster
gracefully (`raft.RemoveServer`) -- the remaining voters keep operating normally -- and are the
first commands in this project with any teardown-side raft membership change at all. `leave`
preserves the composite cluster data dir on disk (so a later `join`/`rejoinnode` back to the same
cluster picks its local state back up); `rm` deletes it and also revokes standing via the same
`cluster-join` permit kind, so a later `join` attempt starts genuinely pending again rather than
being silently re-admitted.

Catalog/dispatch targets (wrap `pkg/kvctl/catalog.go`+`dispatch.go`, the `mage`-side mirror of
`mobile/kvmobile`'s Group/Command/execution layer — see README's `kvmobile` section for the
concepts, since the two are otherwise identical):

```bash
mage creategroup/updategroup/deletegroup/getgroup/listgroups <args>
mage requestgroupparticipation/confirmgroupparticipation/revokegroupparticipation <groupID> <peerID> ...
mage isgroupparticipant <groupID>
mage createcommand/updatecommand/deletecommand/getcommand/listcommands <args>
mage submitcommand/getcommandrequest/listcommandrequests <args>
mage listexecutions <peerID>
mage appendcommandlog/querycommandlog/latestcommandlog <args>
```

`mage -l` prints every target's full usage/argument list. Deliberately not ported: kvmobile's
QR-scan convenience (`getgroup`+`listcommands` already cover the same ground) and
`WatchExecute`/`WatchCommandLog`'s callback-driven poll loops, which don't fit a one-shot `mage`
invocation — poll `querycommandlog`/`latestcommandlog`/`pollexecute` yourself if a script needs to
watch.

End-to-end pipeline (`test/e2e/testdata.json` is the source of truth; see README's "End-to-end
tests / deploy pipeline" section for the full design):

```bash
mage e2e:current     # run rows newer than the last published version — this is the pre-push gate
mage e2e:all         # run every recorded row
mage e2e:bootstrap   # deploy/confirm the shared SSH leader
mage e2e:deletenode <nodeID>
mage e2e:destroyall
mage githooks:install   # points core.hooksPath at scripts/git-hooks/pre-push (runs e2e:current)
```

There is no configured linter (no `.golangci.yml`); `go vet`/`gofmt` are the baseline.

## Architecture

Every "user"-to-"raft node" hop in this project — local IPC and the network hop alike — speaks the
exact same wire struct, defined once in `api/shmevent.capnp` and code-generated into both
`pkg/shmevent` (Go) and `web-app/src/shmevent.rs` (Rust). One `event` byte, `sourceId`/
`destinationId` relational references, a raw `value`, a CRC32, an Ed25519 `signature`, and a
correlation `id`. A `Set` decomposes into a linked `SetKey`+`SetField` pair, a `Get` is a one-shot
`GetField`, and `GetPublicKey`/`GetPrivateKey` let a caller with no key yet bootstrap into the same
key the node itself holds. Understanding this struct is prerequisite to touching almost any client
or transport code here — see `api/shmevent.capnp`'s doc comment for the full design.

- `pkg/daemon` (`cmd/kvnode`) — the long-running node process: libp2p host, raft instance backed by
  `pkg/kvfsm`/`pkg/store`, and a `pkg/ipc` server for local control. A node has no leader/follower
  role until it gets an `EventAdd`: bootstrap as sole leader, or join an existing one.
- `pkg/ipc` — request/response IPC between a short-lived CLI process and the daemon, over shmring
  ring buffers carrying `pkg/shmevent.Msg`. `ipc.go` is the desktop (named shared-memory) transport;
  `ipc_android.go` is the Android transport (`ASharedMemory`, no named rendezvous — client and
  daemon must share a process). `pkg/shmclient` implements the caller-side SetKey+SetField/GetField
  orchestration and the `GetPrivateKey` bootstrap on top of it.
- `pkg/kvctl` / `cmd/kvctl-cli` — client logic for spawning/bootstrapping nodes and issuing
  set/get. `kvctl-cli` needs no Go toolchain, meant to run next to an already-built `kvnode` on a
  remote deployment target reached over SSH.
- `mobile/kvmobile` — `gomobile`-bindable entry point running the follower daemon in-process inside
  the Android app (`android-app/`). The leader address is baked in at build time via an `-ldflags
  -X` var (a mobile app has no operator to type a peer ID at runtime). Every `submit` forwards from
  this never-leader follower to whichever peer is currently leader over
  `pkg/daemon.ForwardProtocolID`.
- `web-app/` — a browser client, Rust compiled to `wasm32-unknown-unknown` over `rust-libp2p`.
  Unlike every other client, it can never be a raft *voter* (a browser sandbox can't accept a raw
  inbound connection), but it holds a circuit-relay v2 reservation and joins as a real raft
  **non-voter (learner)**, reimplementing `hashicorp/raft`'s `NetworkTransport` msgpack wire
  protocol byte-for-byte to receive genuine `AppendEntries` replication. See `web-app/README.md`
  for its architecture — it's kept current and detailed, same standard as the top-level README.
- `magefile.go` — desktop convenience targets; `cmd/magefile.go` — an older relay/echo demo
  (`Relay`/`Client`/`TestP2PRelay`) unrelated to the raft cluster path.
- `thirdparty/anet` — a local patched copy of `github.com/wlynxg/anet` (pinned via `replace` in
  `go.mod`), dropping a `//go:linkname` against a private stdlib symbol that no longer matches
  Go 1.25's `net` package layout. See README's "Vendored dependency patch" section before touching.

## Node connectivity policy

`configs/bootstrap-nodes.json` is the catalog of already-deployed nodes running `-relay-service`
(circuit-relay v2 points) — read via `mage bootstrapnodes`. Any node that can't guarantee it's
directly dialable by the rest of the cluster (a phone, a browser, a dev laptop on a LAN/firewall/
dynamic IP — anything that isn't a stable, port-open host like the `configs/bootstrap-nodes.json`
entries) **must** join with its relay peer set to one of those bootstrap nodes — `-relay-peer`/
`Config.RelayPeer` on desktop, `mobile/kvmobile.relayMultiaddr` at build time for Android — rather
than relying on direct dial-back working. This is what makes the initial join (`AddVoter`/
`AddNonvoter`) and ongoing `AppendEntries` replication succeed even when the joining node has no
address anyone else can reach directly: `newHost` reserves a circuit-relay v2 slot through the
named bootstrap node and advertises the resulting `/p2p-circuit` address instead.

**Known gap, found running a real 3-node cluster (desktop + remote bootstrap node + Android) on
2026-07-12/13:** a follower's forwarded `Set` (`pkg/daemon.ForwardProtocolID`, `handleForwardSetStream`)
dials whoever is the *current raft leader* directly, using that leader's own self-advertised
address — it has no relay fallback. If the current leader is itself not reliably reachable (e.g. a
dev laptop whose local firewall allows ICMP but rejects inbound TCP from LAN peers — confirmed
directly: `ping` succeeded, `nc -z <ip> <port>` returned "No route to host"), every follower's
writes fail with `failed to open stream: context deadline exceeded`, even though join and
replication (which *do* have relay/direct fallback baked into how addresses are advertised) keep
working fine and reads stay correct. Until the forward path itself gains a relay fallback, keep
raft leadership on a node from `configs/bootstrap-nodes.json` (or another host with a real,
firewall-open address) rather than letting an ad hoc dev machine become leader.

### Stale docs

`docs/getting-started.md`, `docs/linux.md`, and `docs/android.md` describe an earlier prototype
(`cmd/client/main.go`, `cmd/relay/main.go`, `pkg/raft.NewP2PNode`) that predates the current
`pkg/daemon`/`shmevent`/`mobile/kvmobile` architecture and does not match the real codebase —
`docs/web.md` explicitly flags this drift for itself and says android.md has the same problem.
Don't treat any of the four as ground truth; use the top-level `README.md` and `web-app/README.md`
instead.

## Testing conventions

- `mage e2e:current` is the pre-push gate once `mage githooks:install` is run; it only re-runs rows
  newer than `published_version` in `test/e2e/testdata.json`, not the whole history.
- Deployed e2e nodes are **never torn down automatically** by any e2e command — that's deliberate,
  so a human can inspect them after a run. Use `mage e2e:deletenode <nodeID>` /
  `mage e2e:destroyall` explicitly when a node is no longer wanted.
- An `add` (raft join) row is inherently one-time; re-running `mage e2e:all` against an
  already-joined node produces an expected "not leader" rejection on that row, not a real failure.
