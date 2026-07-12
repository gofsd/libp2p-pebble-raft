# libp2p-kv-raft

A distributed key-value store: [hashicorp/raft](https://github.com/hashicorp/raft) consensus
running over [libp2p](https://github.com/libp2p/go-libp2p) transport, with
[SQLite](https://sqlite.org/) (via the pure-Go [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)
driver, so no CGO toolchain is needed) as the on-disk store for the replicated state
machine. Nodes can run on separate machines (including behind NAT/cellular, e.g. an Android
phone) and are driven locally over `github.com/gofsd/shmring` shared-memory IPC rather than a
network-facing RPC port.

## Architecture

- `pkg/daemon` — the long-running node process (`cmd/kvnode`): a libp2p host, a raft instance
  backed by `pkg/kvfsm`/`pkg/store`, and a `pkg/ipc` server for local control.
- `api/shmevent.capnp` — the single [Cap'n Proto](https://capnproto.org/)-encoded wire struct every
  "user"-to-"raft node instance" hop speaks: `pkg/ipc`'s local shared memory, and
  `pkg/daemon.ClientProtocolID`'s network hop for a remote browser learner. One `event` byte,
  `sourceId`/`destinationId` relational references, a raw `value`, a CRC32, an Ed25519 `signature`,
  and a correlation `id` — a Set decomposes into a linked `SetKey`+`SetField` pair, a Get is a
  one-shot `GetField`, and `GetPublicKey`/`GetPrivateKey` are how a caller with no key yet
  bootstraps into the same key the node itself holds. `pkg/shmevent` (Go) and `web-app/src/shmevent.rs`
  (Rust) are both generated from this identical schema. See its doc comment for the full design.
- `pkg/ipc` — request/response IPC between a short-lived CLI process and the daemon, over shmring
  ring buffers carrying `pkg/shmevent.Msg`. `ipc.go` is the desktop (named shared-memory) transport;
  `ipc_android.go` is the Android transport (`ASharedMemory`, no named rendezvous, so client and
  daemon must share a process — see that file's doc comment). `pkg/shmclient` implements the
  caller-side SetKey+SetField/GetField orchestration and the `GetPrivateKey` bootstrap on top of it.
- `pkg/kvctl` / `cmd/kvctl-cli` — client logic for spawning/bootstrapping nodes and issuing
  set/get requests. `kvctl-cli` is a no-Go-toolchain-required binary meant to run next to an
  already-built `kvnode` binary on a remote deployment target (e.g. a VPS reached over SSH).
- `mobile/kvmobile` — the `gomobile`-bindable entry point that runs the follower daemon
  in-process inside an Android app (see `android-app/`).
- `magefile.go` — desktop convenience targets (`mage addnode`, `mage set`, ...) that wrap
  `pkg/kvctl` for local development.
- `web-app/` — a browser client, in Rust compiled to `wasm32-unknown-unknown` over `rust-libp2p`
  (see [Client in a browser](#client-in-a-browser)); unlike every other client here it never
  *votes*, but it does run a real hashicorp/raft non-voter (learner), reimplementing
  `NetworkTransport`'s msgpack wire protocol to receive genuine `AppendEntries` replication.

A node has no leader/follower role until it receives an `EventAdd` request (`pkg/shmevent`):
bootstrap as the cluster's sole leader, or join an existing leader (given as a bare peer ID
registered on the same machine, or a full multiaddr for a leader on another machine).

## Running a cluster

### Leader on a remote machine (over SSH)

The remote machine needs no Go toolchain — cross-compile (or build natively) `kvnode` and
`kvctl-cli`, copy them over, then bootstrap:

```bash
GOOS=linux GOARCH=amd64 go build -o kvnode ./cmd/kvnode
GOOS=linux GOARCH=amd64 go build -o kvctl-cli ./cmd/kvctl-cli
scp kvnode kvctl-cli user@remote:/opt/kvstore/bin/

ssh user@remote 'KVSTORE_HOME=/opt/kvstore /opt/kvstore/bin/kvctl-cli addnode \
  -bin /opt/kvstore/bin/kvnode -listen-port 4001 -relay-service'
```

`-relay-service` makes this node act as a circuit-relay v2 point (needed for followers with no
directly-dialable address of their own, e.g. a phone on cellular) and forces it to advertise
itself as publicly reachable. `-listen-port` pins the port so it survives restarts.
`KVSTORE_HOME` controls where the registry/node data lives (defaults to
`~/.libp2p-kv-raft`); set it explicitly and pass it on every subsequent `kvctl-cli` call
against that install.

Print the leader's multiaddr for followers to join against:

```bash
ssh user@remote 'cat /opt/kvstore/registry.json'   # listen_addrs includes the public multiaddr
```

### Follower on the local machine

```bash
mage addfollower "/ip4/<remote-ip>/tcp/4001/p2p/<leader-peer-id>"
mage set mykey myvalue
mage get mykey
```

`mage resumenode <peerID>` restarts an existing node in place from its persisted raft state (no
leader coordination needed, as long as its address hasn't changed). `mage rejoinnode <leaderAddr>
<peerID>` restarts it *and* re-sends the join request — use this if the node's address changed or
a new leader needs to know about it. Note a 2-voter cluster has no fault tolerance: if either side
is down for a while, the other cannot commit and eventually can't win an election either;
bringing the down side back with `resumenode`/`rejoinnode` lets them re-elect on their own.

### Follower on Android

The Android app (`android-app/`) runs the same follower daemon in-process via
`mobile/kvmobile`, bound as a `gomobile` AAR. The leader to join is baked in at build time (a
mobile app has no operator to type a peer ID at runtime):

```bash
export ANDROID_NDK_HOME=<path-to-ndk>   # e.g. $ANDROID_HOME/ndk/<version>
LEADER_ADDR="/ip4/<remote-ip>/tcp/4001/p2p/<leader-peer-id>"

gomobile bind -target=android -androidapi 26 \
  -ldflags "-X github.com/gofsd/libp2p-kv-raft/mobile/kvmobile.leaderMultiaddr=$LEADER_ADDR" \
  -o android-app/app/libs/kvmobile.aar ./mobile/kvmobile

cd android-app && gradle assembleDebug   # no wrapper checked in; use a local gradle install
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

`-androidapi` **must be 26 or higher** — the shmring Android backend uses `ASharedMemory_create`,
which the NDK headers only declare from API 26 onward; building against a lower target silently
hides the declaration and fails with a confusing `could not determine what
C.ASharedMemory_create refers to` linker error rather than a clear availability error.

The app's UI (`MainActivity`) is a thin wrapper over `Kvmobile.start/submit/get`: `start` brings
up the daemon and joins the cluster, `submit`/`get` go through the daemon's IPC exactly like the
desktop CLI, just over the Android shared-memory transport instead of named shared memory. Every
`submit` is forwarded from this (never-leader) follower to whichever peer is currently leader,
over `pkg/daemon.ForwardProtocolID`.

**MIUI/Xiaomi devices**: `adb install` can fail with `INSTALL_FAILED_USER_RESTRICTED` even with
"Unknown sources" allowed — there's a separate Developer Options toggle, **"Install via USB"**,
that must also be enabled.

**Relay reservations for NAT'd followers**: `pkg/daemon.Config.RelayPeer` (and the mirroring
`mobile/kvmobile.relayMultiaddr` build-time var) let a follower with no directly-dialable address
(e.g. a phone behind carrier-grade NAT) proactively reserve a circuit-relay v2 slot through the
leader, so a raft voter that nothing can dial directly can still be reached. This previously
failed the join handshake with a libp2p stream-protocol-negotiation error (`0x1001`): the relay
reservation wait was sitting between opening the join stream and writing to it, easily outlasting
the remote's negotiation timeout. It's fixed — `join()` now waits for the reservation before
opening the stream at all, and the node also forces itself privately reachable when `RelayPeer` is
set rather than leaving that judgment to AutoNAT — and covered by a real relay+leader+follower
cluster test (`pkg/daemon.TestJoinThroughRelay`). A plain direct join (no `relayMultiaddr`) has
also been tested working from a phone on cellular data, joining a publicly reachable leader.

### Client in a browser

Unlike the desktop CLI and the Android app, a browser tab can never be a raft *voter*: a voter's
transport must be independently dialable by any other voter at any time, and a browser sandbox has
no way to accept a raw inbound connection. But it turns out a tab *can* be a raft **non-voter
(learner)** once it holds a circuit-relay v2 reservation — the same mechanism a phone behind
carrier-grade NAT already relies on (see [Relay reservations for NAT'd
followers](#follower-on-android) above) — so `web-app/` is a real (if non-voting) member of the
cluster, in Rust compiled to `wasm32-unknown-unknown` over `rust-libp2p`: it reimplements
`hashicorp/raft`'s `NetworkTransport` msgpack wire protocol to receive genuine `AppendEntries`
replication, backed by real SQLite (`sqlite-wasm-rs`) for the replicated log and kv table. Joining
happens over `pkg/daemon.ClientProtocolID`, speaking `pkg/shmevent`'s capnp struct: the browser
first fetches the target's Ed25519 key (`EventGetPrivateKey`, unsigned — the one bootstrap
exception), then sends a signed `EventSetKey`+`EventAdd` pair (own peer id, then own reserved
address) to `handleAddLearner`, which calls `raft.AddNonvoter` — forwarding to the real leader
server-side if the dialed node isn't it, one hop, mirroring how a voter's own join request forwards
(`pkg/daemon.ForwardJoinProtocolID`). A Set still forwards to the leader the same way (as a signed
`EventSetKey`+`EventSetField` pair); a Get reads this tab's own locally-replicated state.

Every node already listens on a browser-reachable WebTransport address (`newHost` adds it
alongside the existing TCP/QUIC listeners); `advertisedAddrs()`/`ready.json` include it
automatically, with its `/certhash` component already appended:

```bash
cat ~/.libp2p-kv-raft/registry.json   # listen_addrs includes .../quic-v1/webtransport/certhash/.../p2p/<peer-id>
```

```bash
cd web-app
npm install
npm run dev   # builds the wasm bundle, then serves on a cross-origin-isolated origin
```

Paste that WebTransport multiaddr into the running page's "Node multiaddr" field and Connect —
unlike Android's build-time-baked leader address (a phone has no operator to type one in), the web
UI takes it at runtime, closer to desktop's `mage addfollower <addr>`. See `web-app/README.md` for
the full architecture and its currently-unverified-in-CI gaps (needs a wasm32 C toolchain for
SQLite, and a real browser + live cluster to exercise end to end).

## Vendored dependency patch

`thirdparty/anet` is a local, patched copy of `github.com/wlynxg/anet` (pinned via a `replace`
directive in `go.mod`). Upstream's Android network-interface code links (`//go:linkname`)
against `net.zoneCache`, a private stdlib symbol; its layout no longer matches Go 1.25's `net`
package, and there is no newer upstream release fixing it. The patch drops the link and keeps the
cache local-only — harmless here, since libp2p only calls `Interfaces()`/`InterfaceAddrs()` for
listing, never anything relying on the standard library's own zone cache being warmed as a side
effect.

## Testing

```bash
mage test          # unit tests
mage integration    # integration tests
mage testall        # all of the above, plus every e2e:all row (see below)
```

### End-to-end tests / deploy pipeline

`test/e2e/testdata.json` is the single source of truth for the e2e suite: a version history,
deterministic Ed25519 identities per platform (desktop/android/web), and a recorded log of test
rows -- each one raw `pkg/shmevent.Msg` sent to a node, with the last run's pass/fail status and
error message. See `pkg/e2edata` for the file format and `pkg/e2erun` for what running a row
actually does per platform (a real locally-spawned `kvnode` for desktop, a real
Playwright-driven browser check for web, and -- since no on-device/emulator build automation
exists yet -- a clearly marked skip for android).

```bash
mage e2e:newversion "<label>"                              # start a new version
mage e2e:addnode desktop                                    # generate a deterministic identity
mage e2e:addtest <nodeID> <event> <id> <sourceID> <destID> <value>  # record a row against it
mage e2e:bootstrap                                           # deploy/confirm the shared leader (SSH)
mage e2e:current                                              # run only rows newer than the last published version
mage e2e:all                                                   # run every recorded row
```

`mage e2e:current` is what runs before every push once installed:

```bash
mage githooks:install   # one-time: points core.hooksPath at scripts/git-hooks/pre-push
```

The shared bootstrap/leader node these tests join against is deployed over SSH to a single,
already-provisioned VPS, into its own isolated directory/port (`pkg/e2erun.BootstrapRemoteDir`,
distinct from any other node manually running on that same host) -- `mage e2e:bootstrap` (or the
first `e2e:current`/`e2e:all` run) is idempotent: it deploys and starts it only if not already up,
and otherwise just confirms it's reachable.
