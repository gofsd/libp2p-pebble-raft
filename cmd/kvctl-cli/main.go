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
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
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
	case "sendevent":
		cmdSendEvent(os.Args[2:])
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
  kvctl-cli sendevent <peerID> <eventJSON>

sendevent sends one raw pkg/shmevent.Msg (JSON-encoded, e.g.
'{"event":4,"value_hex":"68656c6c6f"}' -- see pkg/e2edata.Event for the
field names, why value is hex not a plain string, and pkg/shmevent's event
constants for the "event" byte) to peerID over the
local shmring transport, signing it with peerID's own key when the event
type requires one (fetched via an unsigned EventGetPrivateKey first). It
prints the JSON response event to stdout and exits non-zero if the response
is EventError (255) or the call itself failed. This is the low-level
primitive the e2e test pipeline drives -- both locally and, since this
binary is the one already cross-compiled and copied to remote deployment
targets, identically over SSH against a remote node.

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

// sendEventTimeout bounds both the optional GetPrivateKey signing-key
// fetch and the event call itself.
const sendEventTimeout = 10 * time.Second

func cmdSendEvent(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli sendevent <peerID> <eventJSON>")
		os.Exit(2)
	}
	peerID := args[0]

	var ev e2edata.Event
	if err := json.Unmarshal([]byte(args[1]), &ev); err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: parse event json: %v\n", err)
		os.Exit(2)
	}
	if ev.ID == 0 {
		ev.ID = randomID()
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendEventTimeout)
	defer cancel()

	var priv shmevent.PrivateKey
	if shmevent.RequiresSignature(ev.EventType) {
		keyResp, err := ipc.Call(ctx, peerID, shmevent.Msg{EventType: shmevent.EventGetPrivateKey, ID: randomID()}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sendevent: fetch signing key: %v\n", err)
			os.Exit(1)
		}
		if keyResp.EventType == shmevent.EventError {
			fmt.Fprintf(os.Stderr, "sendevent: fetch signing key: %s\n", keyResp.Value)
			os.Exit(1)
		}
		priv = shmevent.PrivateKey(keyResp.Value)
	}

	msg, err := ev.ToMsg()
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: %v\n", err)
		os.Exit(2)
	}
	resp, err := ipc.Call(ctx, peerID, msg, priv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: %v\n", err)
		os.Exit(1)
	}

	out, err := json.Marshal(e2edata.EventFromMsg(resp))
	if err != nil {
		fmt.Fprintf(os.Stderr, "sendevent: encode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
	if resp.EventType == shmevent.EventError {
		os.Exit(1)
	}
}

// randomID returns a random non-zero id -- 0 is reserved meaning
// "SourceID/DestinationID not used" (see api/shmevent.capnp), so a real
// message's own id avoids it too.
func randomID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}
