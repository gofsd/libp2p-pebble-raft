// Command kvnode is the long-running node daemon spawned by `mage addnode`.
// It has no notion of leader/follower at startup; that is decided later by
// an ipcproto.ActionAdd request delivered over pkg/ipc (see pkg/daemon).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofsd/libp2p-pebble-raft/pkg/daemon"
)

func main() {
	dataDir := flag.String("data-dir", "", "node data directory (identity key, pebble, raft)")
	keyPath := flag.String("key-path", "", "path to this node's libp2p identity key")
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
		DataDir: *dataDir,
		KeyPath: *keyPath,
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "kvnode: %v\n", err)
		os.Exit(1)
	}
}
