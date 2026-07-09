# libp2p-pebble-raft

A distributed key-value store: [hashicorp/raft](https://github.com/hashicorp/raft) consensus
running over [libp2p](https://github.com/libp2p/go-libp2p) transport, with
[Pebble](https://github.com/cockroachdb/pebble) as the on-disk store for the replicated state
machine. Nodes can run on separate machines (including behind NAT/cellular, e.g. an Android
phone) and are driven locally over `github.com/gofsd/shmring` shared-memory IPC rather than a
network-facing RPC port.

## Architecture

- `pkg/daemon` — the long-running node process (`cmd/kvnode`): a libp2p host, a raft instance
  backed by `pkg/kvfsm`/`pkg/store`, and a `pkg/ipc` server for local control.
- `pkg/ipc` — request/response IPC between a short-lived CLI process and the daemon, over shmring
  ring buffers. `ipc.go` is the desktop (named shared-memory) transport; `ipc_android.go` is the
  Android transport (`ASharedMemory`, no named rendezvous, so client and daemon must share a
  process — see that file's doc comment).
- `pkg/kvctl` / `cmd/kvctl-cli` — client logic for spawning/bootstrapping nodes and issuing
  set/get requests. `kvctl-cli` is a no-Go-toolchain-required binary meant to run next to an
  already-built `kvnode` binary on a remote deployment target (e.g. a VPS reached over SSH).
- `mobile/kvmobile` — the `gomobile`-bindable entry point that runs the follower daemon
  in-process inside an Android app (see `android-app/`).
- `magefile.go` — desktop convenience targets (`mage addnode`, `mage set`, ...) that wrap
  `pkg/kvctl` for local development.

A node has no leader/follower role until it receives an `ActionAdd` request: bootstrap as the
cluster's sole leader, or join an existing leader (given as a bare peer ID registered on the same
machine, or a full multiaddr for a leader on another machine).

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
`~/.libp2p-pebble-raft`); set it explicitly and pass it on every subsequent `kvctl-cli` call
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
  -ldflags "-X github.com/gofsd/libp2p-pebble-raft/mobile/kvmobile.leaderMultiaddr=$LEADER_ADDR" \
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

**Known caveat — relay reservations for NAT'd followers**: `pkg/daemon.Config.RelayPeer` (and the
mirroring `mobile/kvmobile.relayMultiaddr` build-time var) exist so a follower with no
directly-dialable address (e.g. a phone behind carrier-grade NAT) can proactively reserve a
circuit-relay v2 slot through the leader. As currently wired, setting it causes the join
handshake to fail with a libp2p stream-protocol-negotiation error (`0x1001`), likely from
resource contention between the relay reservation and the join stream to the same peer — it needs
further investigation before it's usable. Leave `relayMultiaddr` unset (the default) for now; a
plain direct join has been tested working from a phone on cellular data, joining a publicly
reachable leader.

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
mage e2e            # end-to-end tests
mage testall        # all of the above
```
