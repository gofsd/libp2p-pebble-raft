// Command kvctl-cli is a plain, no-Go-toolchain-required client for
// pkg/kvctl, meant to run alongside an already-built kvnode binary on a
// machine that doesn't have this repo's source or a Go toolchain -- e.g. a
// remote deployment target reached over SSH, where both binaries were
// cross-compiled elsewhere and copied over.
//
// Unlike the mage targets (which build kvnode from source via
// kvctl.AddNode), addnode here always takes a pre-built binary path via
// -bin and calls kvctl.AddNodeWithBinary.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "addnode":
		cmdAddNode(os.Args[2:])
	case "resumenode":
		cmdResumeNode(os.Args[2:])
	case "use":
		cmdUse(os.Args[2:])
	case "set":
		cmdSet(os.Args[2:])
	case "get":
		cmdGet(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  kvctl-cli addnode -bin <kvnode-binary-path> [-listen-port N] [-relay-service] [raft flags] [leaderPeerIDOrMultiaddr] [ownPeerID]
  kvctl-cli resumenode -bin <kvnode-binary-path> [raft flags] <ownPeerID>
  kvctl-cli use <peerID>
  kvctl-cli set <key> <value>
  kvctl-cli get <key>

raft flags (all default to hashicorp/raft's own WAN-appropriate values):
  -raft-heartbeat-timeout, -raft-election-timeout, -raft-commit-timeout, -raft-leader-lease-timeout`)
}

// raftTimeoutFlags registers the four raft timing flags shared by addnode
// and resumenode, and returns a function that turns whichever were set
// into "-flag value" pairs for the spawned kvnode's command line.
func raftTimeoutFlags(fs *flag.FlagSet) func() []string {
	heartbeatTimeout := fs.Duration("raft-heartbeat-timeout", 0, "raft heartbeat timeout (0 = default, 1s)")
	electionTimeout := fs.Duration("raft-election-timeout", 0, "raft election timeout (0 = default, 1s)")
	commitTimeout := fs.Duration("raft-commit-timeout", 0, "raft commit timeout (0 = default, 50ms)")
	leaderLeaseTimeout := fs.Duration("raft-leader-lease-timeout", 0, "raft leader lease timeout (0 = default, 500ms)")

	return func() []string {
		var extra []string
		if *heartbeatTimeout != 0 {
			extra = append(extra, "-raft-heartbeat-timeout", heartbeatTimeout.String())
		}
		if *electionTimeout != 0 {
			extra = append(extra, "-raft-election-timeout", electionTimeout.String())
		}
		if *commitTimeout != 0 {
			extra = append(extra, "-raft-commit-timeout", commitTimeout.String())
		}
		if *leaderLeaseTimeout != 0 {
			extra = append(extra, "-raft-leader-lease-timeout", leaderLeaseTimeout.String())
		}
		return extra
	}
}

func cmdAddNode(args []string) {
	fs := flag.NewFlagSet("addnode", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	listenPort := fs.Int("listen-port", 0, "TCP/QUIC port for the new node to listen on (0 = ephemeral)")
	relayService := fs.Bool("relay-service", false, "make the new node act as a relay for others (only for nodes with a real public address)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "addnode: -bin is required")
		os.Exit(2)
	}

	extra := raftArgs()
	if *listenPort != 0 {
		extra = append(extra, "-listen-port", strconv.Itoa(*listenPort))
	}
	if *relayService {
		extra = append(extra, "-relay-service")
	}

	peerID, err := kvctl.AddNodeWithBinary(*binPath, extra, fs.Args()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "addnode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(peerID)
}

func cmdResumeNode(args []string) {
	fs := flag.NewFlagSet("resumenode", flag.ExitOnError)
	binPath := fs.String("bin", "", "path to a pre-built kvnode binary (required)")
	raftArgs := raftTimeoutFlags(fs)
	fs.Parse(args)

	if *binPath == "" {
		fmt.Fprintln(os.Stderr, "resumenode: -bin is required")
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli resumenode -bin <path> [raft flags] <ownPeerID>")
		os.Exit(2)
	}

	peerID, err := kvctl.ResumeNodeWithBinary(*binPath, fs.Arg(0), raftArgs())
	if err != nil {
		fmt.Fprintf(os.Stderr, "resumenode: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(peerID)
}

func cmdUse(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli use <peerID>")
		os.Exit(2)
	}
	if err := kvctl.Use(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "use: %v\n", err)
		os.Exit(1)
	}
}

func cmdSet(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli set <key> <value>")
		os.Exit(2)
	}
	if err := kvctl.Set(args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "set: %v\n", err)
		os.Exit(1)
	}
}

func cmdGet(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli get <key>")
		os.Exit(2)
	}
	value, err := kvctl.Get(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(value)
}
