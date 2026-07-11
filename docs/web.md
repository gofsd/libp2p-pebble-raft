# Web Integration Guide

See [`web-app/README.md`](../web-app/README.md) for the actual, current browser client and its
architecture. Summary: `web-app/` is a Rust crate compiled to `wasm32-unknown-unknown`, running a
real (non-voting) `hashicorp/raft` learner in the browser -- [`rust-libp2p`](https://github.com/libp2p/rust-libp2p)
dials a running `kvnode` over WebTransport, reserves a circuit-relay v2 slot so the leader can
`Dial()` back to the tab, and joins the actual raft configuration via a new `ClientProtocolID`
`ActionAdd` path (`pkg/daemon.handleAddLearner`) that calls `raft.AddNonvoter`. From there the tab
receives genuine `AppendEntries` replication -- reimplementing `hashicorp/raft`'s `NetworkTransport`
msgpack wire format byte-for-byte (verified against real fixtures pulled from `go-msgpack/v2`'s own
test data) -- and answers `Get` from its own locally-applied SQLite-backed state, not a forwarded
read. A Set still forwards to the real leader server-side over `ClientProtocolID`, exactly like a
follower's own forwarded Set. `pkg/daemon.newHost`'s WebTransport listener is what makes any of
this dialable from a browser sandbox in the first place.

This supersedes an earlier version of this client that was a thin `js-libp2p` client only ever
issuing forwarded Set/Get, never joining raft at all -- a browser tab genuinely can't be a raft
*voter* (a voter's transport must be independently dialable by any other voter at any time), but it
turns out it *can* be a non-voting learner once it holds a relay reservation, the same mechanism an
Android device behind carrier-grade NAT already relies on (see `pkg/daemon.Config.RelayPeer`'s doc
comment).

This file previously (before that) described a different, never-implemented approach (a Go binary
compiled to `GOOS=js GOARCH=wasm`, connecting out over a WebSocket relay). That never matched
anything in this repository (there was no `cmd/client`, and `go-libp2p`/`hashicorp/raft` compiled
to `js/wasm` have no working browser transport regardless) — it was early, aspirational scaffolding
never reconciled with the real architecture that solidified in `pkg/daemon`/`mobile/kvmobile`. See
[`docs/android.md`](android.md) for the same caveat on the Android doc, which has the equivalent
drift relative to the real `mobile/kvmobile` implementation.
