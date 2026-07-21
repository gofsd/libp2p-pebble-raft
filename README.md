# libp2p-kv-raft

A distributed key-value store: [hashicorp/raft](https://github.com/hashicorp/raft) consensus
running over [libp2p](https://github.com/libp2p/go-libp2p) transport, with
[SQLite](https://sqlite.org/) (via the pure-Go [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)
driver, so no CGO toolchain is needed) as the on-disk store for the replicated state
machine. Nodes can run on separate machines (including behind NAT/cellular, e.g. an Android
phone) and are driven locally over `github.com/gofsd/shmring` shared-memory IPC rather than a
network-facing RPC port.

Integrating this repo as a dependency from another Go project (or driving it as a subprocess from
any language)? See [`docs/library-usage.md`](docs/library-usage.md) for the API-focused guide;
this file covers this repo's own architecture and `mage` workflow instead.

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

By default a `-relay-service` node lets *any* peer reserve a slot or open a relayed circuit
through it. Add `-require-permit-for-relay` to restrict that to peers with a confirmed
permit record: an operator (or any current raft voter) runs

```bash
mage requestpermit peer <peerID> ""
mage confirmpermit peer <peerID>
```

(`ConfirmPermit` only takes effect if run against a node that is itself a current raft
voter) before that peer can reserve a relay slot or dial through this node's relay. This is
independent of `-require-permit-for-remote` (not yet exposed as a flag), which instead gates
remote `Set`/`Get`/etc. requests over `ClientProtocolID` — a node can require a permit for one
without the other.

The same confirmed permit record also doubles as the allow-list for `EventExecute`
(`mage execute <destPeerID> <value>` / `mage pollexecute`, a direct unreplicated
peer-to-peer notification between two node processes — see `pkg/shmevent`'s
`EventExecute` doc comment) when a node is started with `-require-permit-for-execute`:
a raft cluster member (voter or learner) can always send one, but any other peer needs
the same `requestpermit`/`confirmpermit` grant as above before its Execute notifications
are delivered rather than silently dropped.

A permit granted this way can be taken back with `mage revokepermit peer <peerID>` (also
voter-only, same as confirm) — it deletes the confirmed record outright, immediately
revoking both relay and Execute access on every node. There's no way to cancel a still
-*pending*, not-yet-confirmed request; it can only be confirmed or overwritten by a
fresh `requestpermit`.

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

`mage addnodewithkey <keyFile>` / `mage addfollowerwithkey <keyFile> <leaderAddr>` are the
`addnode`/`addfollower` equivalents for provisioning a node under a specific, already-known Ed25519
identity — `<keyFile>` is a file in `identity.key`'s own hex-encoded format (e.g. a backed-up copy
of a previous node's key) — instead of always minting a fresh one. The resulting peer id is
whatever that key derives to, not a new random one.

`mage deletenode <peerID>` permanently removes a node's on-disk state (identity key, sqlite store,
raft log/snapshots — its whole data directory under the registry) and its entry in
`registry.json`. It refuses while that node's daemon process still appears to be running — stop it
first — since deleting files out from under a live process would corrupt them; unlike
`e2e:deletenode`, it never kills anything itself.

#### Changing which cluster a node belongs to: `join`/`leave`/`rm`

`addnode`/`addfollower` above always mint a *new* identity. `mage join <targetPeerID>` instead
lets the *current* node (`mage use`) — already running its own default, solo single-node cluster
— join a different cluster under that same identity, switching its active data directory to
`<own-peer-id>-<target-peer-id>` (stopping and restarting the local daemon process as needed; the
same directory-naming scheme `rejoinnode` already uses for an existing identity). Whether this is
admitted immediately or requires a separate approval step depends entirely on the target
daemon's own `-require-confirm-for-join` flag (`Config.RequireConfirmForJoin`, default off):

```bash
mage join <targetPeerID>                    # ask targetPeerID's cluster to admit the current node
mage confirmpermit cluster-join <peerID>    # (only if the target requires it) admit a pending join
```

With `-require-confirm-for-join` unset, `join` behaves exactly like `addfollower` under an existing
identity — immediate `raft.AddVoter`/`AddNonvoter`, no separate step. With it set, a join request
only lodges a pending `shmevent.KindClusterJoin` record (the same pending/confirmed system-record
machinery `requestpermit`/`confirmpermit` already use, just a different `kind`); the joining node
is *not* yet a raft member. **Any current raft voter — not just the leader** — can then run `mage
confirmpermit cluster-join <peerID>` against that cluster to actually admit it; `mage
revokepermit cluster-join <peerID>` deletes a still-pending or already-confirmed record outright
(same voter-only restriction as every other permit kind).

`mage leave <peerID>` asks that cluster to remove peerID via `raft.RemoveServer` — a graceful
shrink; the remaining voters keep operating normally, exactly like hashicorp/raft already
tolerates any minority of members going offline — then restarts peerID's daemon back on its own
default solo data directory. The composite cluster directory is left on disk untouched, so a
later `join`/`rejoinnode` back to the same cluster picks its local state back up.

`mage rm <peerID>` does everything `leave` does, plus revokes peerID's `cluster-join` standing
with that cluster (so a *later* `join` attempt against it starts genuinely pending again, not
auto-admitted by a stale confirmed record) and deletes the composite cluster directory outright —
`deletenode`'s counterpart for "leave a cluster and don't keep its local data around", as opposed
to `deletenode`'s "erase this identity entirely."

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

cd android-app && ./gradlew assembleDebug
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

`Kvmobile` runs exactly one daemon per process, so `Start(dataDir)`/`StartWithKey(dataDir, keyHex)`,
`Stop()`, and `Delete(dataDir)` are the Android equivalents of desktop's `addnode`/`addnodewithkey`/
`use`/`deletenode`, adapted to that constraint: `StartWithKey` provisions `dataDir`'s identity from
an existing `identity.key`-format hex string at runtime instead of always minting a fresh one
(refusing if `dataDir` already holds a *different* identity); `Stop` shuts the current daemon down
so a different `dataDir`/identity can be started next — there's no multi-node registry to pick a
"current" one from the way desktop's `mage use <peerID>` does, so switching is just `Stop()` then
`Start`/`StartWithKey` against the target directory; `Delete` wipes a `dataDir` outright and refuses
while a daemon is running against it, same safety rule as `mage deletenode`.

`Leave()` asks the cluster the currently-running device is joined to to remove it
(`raft.RemoveServer` — a graceful shrink, the remaining voters keep operating normally) and then
stops the daemon, mirroring desktop's `mage leave`. Unlike desktop, there's no solo/default
cluster to fall back to afterward — `kvmobile`'s daemon always joins the build-time-baked-in
`leaderMultiaddr` — so a subsequent `Start`/`StartWithKey` just attempts to rejoin the very same
cluster. `Rm()` does everything `Leave` does, plus revokes this device's `cluster-join` standing
(so that later rejoin attempt needs a fresh confirmation rather than being silently re-admitted,
if the leader requires one) and deletes the joined cluster's local data subdirectory specifically
— never the identity key at `dataDir` itself, same distinction desktop's `mage rm` draws against
`mage deletenode`.

`Kvmobile` also binds the permit and direct-notification desktop commands, against whichever
device is currently running (Start's session, same as Submit/Get): `RequestPermit`/`ConfirmPermit`/
`RevokePermit(kind, targetPeerID[, metadata])` (`kind` is `"peer"` or `"bootstrap"`, mirroring
`mage requestpermit`/`confirmpermit`/`revokepermit`) and their `*LogPermit` counterparts for
`pkg/logrecord` access (`mage requestlogpermit`/`confirmlogpermit`/`revokelogpermit`); `Execute`/
`PollExecute` for the raft-bypassing peer-to-peer `EventExecute` notification (`mage execute`/
`pollexecute`) — `PollExecute` returns a JSON envelope (`{"pending":true,"sender_peer_id":"...",
"value":"..."}` or `{"pending":false}`) since gomobile bindings only support one non-error return
value, unlike `pkg/kvctl.PollExecute`'s 4-value Go signature. `LogAppend(kind, unitID, fieldsJSON,
narrative)`/`LogQuery(kind, unitID, since, until, limit)` are the `pkg/logrecord` read/write
counterparts (`mage logappend`/`logquery`) — `LogQuery` likewise returns a single JSON array string
(`"[]"` when nothing matches, never `null`) instead of `pkg/kvctl.LogQuery`'s `[]logrecord.Record`.

`WatchExecute(cb ExecuteCallback)`/`StopWatchExecute()` push `EventExecute` notifications to the
caller instead of requiring a `PollExecute` timer: `ExecuteCallback` is a Go interface Kotlin
implements (gomobile's reverse-binding direction — the only `kvmobile` API that isn't plain
string-in/string-out), and `WatchExecute` runs a background loop calling `cb.OnNotification`
per notification, in delivery order, on its own goroutine (Kotlin implementations must marshal
back onto the UI thread themselves before touching views). A single registration survives a
`Stop`/`Start` identity switch with no need to call `WatchExecute` again — the loop just waits
whenever no daemon is currently running rather than exiting. `PollExecute` still exists for a
one-shot manual drain; `WatchExecute` is the continuous-delivery alternative, e.g. to drive a
live "command execution log" view fed by whichever peer is running the command (see that peer's
own `LogAppend` calls, watched for and re-fetched via `LogQuery` on each poke).

`kvmobile` also has a `Group`/`Command` catalog layer (`catalog.go`), built entirely on
`LogAppend`/`EventListRange` — no new capnp wire schema: `CreateGroup`/`UpdateGroup`/`DeleteGroup`,
`GetGroup`/`ListGroups`, and the `Command` counterparts `CreateCommand`/`UpdateCommand`/
`DeleteCommand`/`GetCommand`/`ListCommands`. Both are `pkg/logrecord.Record` chains — append-only,
"update" is a fresh revision under the same ID, "delete" a tombstone revision, readers always fold
down to the latest. A `Command` names a `TargetPeerID` (who executes it) and a `FormSchema` (JSON
`[]FormField`) describing the inputs its submission form should collect — `kvmobile` only stores
and discovers a `Command`'s definition, it doesn't interpret or run one.

"Participant of group G" is `IsGroupParticipant(G)`: a confirmed `KindLogPermit` record for
`logKind = "command:"+G`, the *same* string `G`'s commands are stored under (see `commandLogKind`)
— granted/revoked via the group-scoped wrappers `RequestGroupParticipation`/
`ConfirmGroupParticipation`/`RevokeGroupParticipation` (thin wrappers over `*LogPermit`). `Group`
listing/reading has no participation gate (a public catalog any cluster member may browse or
propose one into); every `Command` operation, reads included, requires it. This is enforced
client-side only, inside `kvmobile` itself — nothing in `pkg/daemon` independently blocks a local
caller from reading/writing its own already-replicated store (`Config.RequirePermitForLog` only
gates a *different* peer's forwarded request), so it holds only as long as callers go through these
bindings rather than around them.

`kvmobile`'s dispatch layer (`dispatch.go`) turns a `Command` from the catalog into an actual
request/response flow, still with no new capnp wire schema. `ResolveQRGroup` decodes a scanned QR
payload (`{"group_id":...}`) into a `GroupView` (a `Group` plus its `ListCommands` result) in one
call. `SubmitCommand(groupID, commandID, inputsJSON)` writes a durable `CommandRequest` under a
per-group log kind and sends the command's `TargetPeerID` a best-effort `Execute` poke, returning
an `instanceID` the caller tracks the dispatch by; `GetCommandRequest`/`ListCommandRequests` read
it back (the latter is a target device's catch-up path for a poke it might have missed). The
target reports progress with `AppendCommandLog(requesterPeerID, instanceID, fieldsJSON,
narrative)`, read back via `QueryCommandLog`/`WatchCommandLog` (a 1.5s poll, accelerated but not
replaced by `AppendCommandLog`'s own poke back to the requester) or `LatestCommandLog(instanceID)`
for just the newest entry.

`ListExecutionsByPeer(peerID)` answers "every command execution touching this peer, across every
group" without iterating `ListCommandRequests` per group: `SubmitCommand` writes a small per-peer
index entry (`commandExecIndexKind`) alongside the `CommandRequest` itself, once for the requester
and once for the target (skipped if they're the same peer), and `ListExecutionsByPeer` walks just
that one peer's index, most-recent-first, capped at 200. The index is deliberately thin — it
stores only `group_id`/`command_id`/a one-byte role code, not `requested_by` (already the record's
own `AuthorPeerID`) or `target_peer_id` (redundant when the role is target; looked up via
`GetCommand` for a requester-role entry instead) — because every `pkg/logrecord` write shares
`pkg/shmevent.ValueSize`'s 512-byte budget across its *key* (which already embeds a full peer id
once for this index, via `commandExecIndexKind`) and value combined; an earlier version of this
index stored both peer ids directly and blew that budget the moment two real ~52-byte peer ids
were involved in the same dispatch.
over `pkg/daemon.ForwardProtocolID`. `Kvmobile.sendEvent` (not used by `MainActivity`, only by the
e2e pipeline's `E2ETest` instrumented test) exposes the same raw `pkg/shmevent` event dispatch
`submit`/`get` are themselves built on, for tests that need the exact event kvctl-cli's `sendevent`
can send on desktop/remote rather than only the higher-level Set/Get shape.

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

**Which node to point `RelayPeer`/`relayMultiaddr` at**: `configs/bootstrap-nodes.json` (read via
`mage bootstrapnodes`) is the catalog of already-deployed `-relay-service` nodes -- any node that
can't guarantee it's directly dialable (a phone, a browser, a dev laptop on a LAN/firewall/dynamic
IP) should reserve its relay slot through one of those rather than assume direct dial-back will
work. See `CLAUDE.md`'s "Node connectivity policy" for a real gap this surfaced: relay only covers
join/replication today, not a follower's forwarded `Set` (`ForwardProtocolID` dials the current
leader directly, no relay fallback) -- so leadership should stay on a bootstrap-nodes.json host,
not an ad hoc dev machine, until that path gets the same fallback.

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

## Log records

`pkg/logrecord` is a generic, replicated structured-record store built on top of the
same raft-backed KV path ordinary `set`/`get` use — for staff journals, situation
reports, casualty reports, or any other append-heavy structured record type an operator
wants to keep. It's deliberately generic: `kind` (e.g. `"sitrep"`, `"journal"`,
`"casrep"`, anything) is a plain string chosen at call time, not a fixed list baked into
the code, and `Record`'s `Fields`/`Narrative` are an open envelope — this package makes
no claim to implementing any report format's real standardized field layout (those vary
by service, nation, and doctrine); populate them however your own reporting standard
requires.

```bash
mage logappend sitrep 1BCT '{"posture":"green"}' "no significant activity"
mage logappend sitrep 1BCT '{"posture":"amber"}' "increased patrol activity, sector 4"
mage logquery sitrep 1BCT             # every sitrep record for unit 1BCT, oldest first
mage logquery sitrep 1BCT "" "" 10    # same, capped at 10 records
```

Every record's key packs `kind` + `unitID` + a nanosecond timestamp so that "every
record of this kind and unit, in a time window, in order" is a plain ordered range scan
(`pkg/store.Store.ScanRange`, exposed remotely as `pkg/shmevent.EventListRange`) —
`logquery`'s optional third/fourth arguments are RFC3339 `since`/`until` bounds. Writing
a record goes through the same raft-replicated `handleSetForward` path an ordinary `Set`
does, under a key inside a reserved namespace (`logrecord.LogKeyPrefix`, alongside the
existing `shmevent.SystemKeyPrefix` reserved for permits/cluster membership) that an
ordinary caller-supplied key can never collide with — reached through its own
`shmevent.EventLogAppend` event rather than plain `Set`, since `Set`/`SetField`
themselves reject that reserved namespace outright.

Two accepted v1 limits, not oversights: a record's JSON encoding shares the same
512-byte `Set` payload budget as everything else (`shmevent.ValueSize`), leaving roughly
400-470 bytes for `Fields`+`Narrative` combined — tight for a long narrative; and there's
no entry cap or rotation policy, since silently dropping old journal entries once a
count limit is hit would be actively wrong for a logbook. Both are left for a future pass
if they turn out to matter in practice.

### Per-kind log access control

By default any caller that already clears `-require-permit-for-remote` (or any local
`mage`/`kvctl-cli` caller, which that flag never gates) may `logappend`/`logquery` records
of *any* kind. Add `-require-permit-for-log` to restrict that further, per (peer, kind)
pair — e.g. a peer permitted for `"sitrep"` still can't touch `"casrep"` unless separately
granted:

```bash
mage requestlogpermit sitrep <peerID> ""
mage confirmlogpermit sitrep <peerID>
```

(`confirmlogpermit`, like `confirmpermit`, only takes effect if run against a node that
is itself a current raft voter.) `mage revokelogpermit sitrep <peerID>` takes a grant back
outright, same rules as `revokepermit`. This is a separate, independent grant from the
plain `requestpermit`/`confirmpermit peer` permission — being permitted for
`-require-permit-for-relay`/`-require-permit-for-remote`/`-require-permit-for-execute`
does not by itself grant any log-kind access, and vice versa. As with
`-require-permit-for-remote`, this only ever restricts a *remote* caller (over
`ClientProtocolID`) — a local caller is always trusted as this node's own operator and is
never gated by it, regardless of raft membership.

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

`test/e2e/testdata.json` is the single source of truth for the e2e suite, and is meant to be read
by a human, not just tooling: a version history stamped with this repo's own semver (one shared
version across every platform, from the same git tags `mage patch`/`minor`/`major` manage),
deterministic Ed25519 identities per platform (desktop/android/web/remote), and a recorded log of
test rows -- each one raw `pkg/shmevent.Msg` sent to a node, printed with a human-readable event
name and a plain-text value rather than the wire bytes (see `pkg/e2edata.Event`'s doc comment for
exactly how, without changing the underlying capnp structure at all) -- with the last run's
pass/fail status and error message. See `pkg/e2edata` for the file format and `pkg/e2erun` for what
running a row actually does per platform: a real locally-spawned `kvnode` for desktop, the SSH
bootstrap leader itself for remote, a real Playwright-driven browser check for web, and for android
a real `gomobile bind` (baking that row's node identity and the live bootstrap address into the
AAR, via the same `kvmobile.SendEvent` raw-event entry point kvctl-cli's `sendevent` exposes on
desktop/remote) + `./gradlew installDebug installDebugAndroidTest` + `adb shell am instrument`
against whatever device/emulator is connected, pulling back a real per-row results file (see
`android-app/app/src/androidTest/.../E2ETest.kt`) -- degrading to a clear Skipped status if
`gomobile`/`adb`/a connected device aren't available at all, and a clear Failed status with the
real diagnostic (not just "exit status 1") if the build/install/instrument step itself fails --
e.g. the exact `INSTALL_FAILED_USER_RESTRICTED` MIUI/Xiaomi restriction noted under [Follower on
Android](#follower-on-android) blocks the *instrumented test* APK's install the same way it can
block a plain `adb install`, needing that same device-side "Install via USB" toggle enabled before
`e2e:current`/`e2e:all` can drive that node for real.

```bash
mage e2e:newversion                                                     # stamp a new version with the current semver
mage e2e:addnode desktop                                                # generate a deterministic identity
mage e2e:addtest <nodeID> <eventName> <id> <sourceID> <destID> <value>  # record a row against it
mage e2e:bootstrap                                                      # deploy/confirm the shared leader (SSH)
mage e2e:bootstrapall                                                   # start the leader, plus every desktop node -- no test rows run
mage e2e:current                                                        # run only rows newer than the last published version
mage e2e:all                                                            # run every recorded row
mage e2e:deletenode <nodeID>                                            # tear down a node's real process/data and remove it
mage e2e:destroyall                                                     # tear down every node at once
```

`eventName` is one of `set_key`, `set_field`, `get_key`, `get_field`, `get_public_key`,
`get_private_key`, `add` (see `pkg/shmevent.EventName`). Deployed nodes are never torn down
automatically -- by `e2e:current`, `e2e:all`, or anything else -- specifically so a human can poke
at them after a run; `e2e:deletenode`/`e2e:destroyall` are the explicit, deliberate commands for
when a node (or every node) is no longer wanted. `e2e:destroyall` tears every node down the same
real way `e2e:deletenode` does (one at a time, continuing past any single node's failure rather than
stopping), then saves the file -- so partial teardown from a failure partway through still sticks
for whichever nodes it did reach.

An `add` row (a raft join) is inherently a one-time operation, same as `mage addnode` itself: once a
node has actually joined, re-running `e2e:all` sends that same join again to an already-voting
member, which `pkg/daemon.handleAdd` correctly rejects ("leader rejected join: ERR: not leader" --
the join target no longer being who to ask). That's an expected re-run artifact, not a pipeline bug
-- a genuinely clean pass needs either a fresh node (`e2e:deletenode` first) or accepting that row
as the one exception on a repeat `e2e:all`. It doesn't affect `e2e:current`/the push gate, since that
only re-runs rows newer than `published_version`.

`mage e2e:current` is what runs before every push once installed:

```bash
mage githooks:install   # one-time: points core.hooksPath at scripts/git-hooks/pre-push
```

The shared bootstrap/leader node these tests join against is deployed over SSH to a single,
already-provisioned VPS, into its own isolated directory/port (`pkg/e2erun.BootstrapRemoteDir`,
distinct from any other node manually running on that same host) -- `mage e2e:bootstrap` (or the
first `e2e:current`/`e2e:all` run) is idempotent: it deploys and starts it only if not already up,
and otherwise just confirms it's reachable.
