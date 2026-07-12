// Command kvnode is the long-running node daemon spawned by `mage addnode`.
// It has no notion of leader/follower at startup; that is decided later by
// a pkg/shmevent EventAdd request delivered over pkg/ipc (see pkg/daemon).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
)

func main() {
	dataDir := flag.String("data-dir", "", "node data directory (identity key, sqlite, raft)")
	keyPath := flag.String("key-path", "", "path to this node's libp2p identity key")
	listenPort := flag.Int("listen-port", 0, "TCP/QUIC port to listen on (0 = ephemeral; pin this for publicly reachable deployments)")
	relayService := flag.Bool("relay-service", false, "act as a circuit-relay v2 point for other nodes and force public reachability (only for nodes with a real public address)")
	heartbeatTimeout := flag.Duration("raft-heartbeat-timeout", 0, "raft heartbeat timeout (0 = hashicorp/raft's own default, 1s -- safe for real networks)")
	electionTimeout := flag.Duration("raft-election-timeout", 0, "raft election timeout (0 = default, 1s)")
	commitTimeout := flag.Duration("raft-commit-timeout", 0, "raft commit timeout (0 = default, 50ms)")
	leaderLeaseTimeout := flag.Duration("raft-leader-lease-timeout", 0, "raft leader lease timeout (0 = default, 500ms)")
	snapshotThreshold := flag.Uint64("raft-snapshot-threshold", 0, "raft log entries since last snapshot before a new one is taken (0 = hashicorp/raft's own default, 8192 -- large for a long-lived leader that new non-voters periodically join, since a join replays the whole log from index 1 up to the last snapshot)")
	snapshotInterval := flag.Duration("raft-snapshot-interval", 0, "how often raft checks whether a snapshot is due (0 = default, 120s)")
	trailingLogs := flag.Uint64("raft-trailing-logs", 0, "log entries a snapshot keeps instead of compacting away (0 = hashicorp/raft's own default, 10240 -- set this alongside -raft-snapshot-threshold, not instead of it: a log smaller than this has nothing eligible for compaction regardless of how often it snapshots)")
	flag.Parse()

	if *dataDir == "" || *keyPath == "" {
		fmt.Fprintln(os.Stderr, "kvnode: -data-dir and -key-path are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	err := daemon.Run(ctx, daemon.Config{
		DataDir:            *dataDir,
		KeyPath:            *keyPath,
		ListenPort:         *listenPort,
		RelayService:       *relayService,
		HeartbeatTimeout:   *heartbeatTimeout,
		ElectionTimeout:    *electionTimeout,
		CommitTimeout:      *commitTimeout,
		LeaderLeaseTimeout: *leaderLeaseTimeout,
		SnapshotThreshold:  *snapshotThreshold,
		SnapshotInterval:   *snapshotInterval,
		TrailingLogs:       *trailingLogs,
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "kvnode: %v\n", err)
		os.Exit(1)
	}
}
