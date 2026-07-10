package raft_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// TestP2PViaRelay is the canonical integration test for end-to-end P2P
// messaging through a Circuit Relay v2 node.
//
// Topology:
//
//	client2 ──(circuit)──> relay ──(circuit)──> client1
//	client2 sends "hello from client2 via relay"
//	client1 echoes "echo: hello from client2 via relay"
//	test asserts the response matches the expected echo
func TestP2PViaRelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	// ── 1. Relay node ───────────────────────────────────────────────────────
	t.Log("Starting relay node…")
	relay, err := raft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("failed to start relay: %v", err)
	}
	defer relay.Host.Close()

	if len(relay.Addrs) == 0 {
		t.Fatal("relay has no addresses")
	}
	relayAddr := relay.Addrs[0]
	t.Logf("relay addr: %s", relayAddr)

	// ── 2. Client1 – echo server ─────────────────────────────────────────────
	t.Log("Starting client1 (echo server)…")
	client1, err := raft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "client1.key"))
	if err != nil {
		t.Fatalf("failed to start client1: %v", err)
	}
	defer client1.Close()
	client1.SetEchoHandler()
	t.Logf("client1 ID: %s", client1.Host.ID())

	// ── 3. Client2 – sender ──────────────────────────────────────────────────
	t.Log("Starting client2 (sender)…")
	client2, err := raft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "client2.key"))
	if err != nil {
		t.Fatalf("failed to start client2: %v", err)
	}
	defer client2.Close()
	t.Logf("client2 ID: %s", client2.Host.ID())

	// ── 4. Send message and verify echo ─────────────────────────────────────
	testMsg := "hello from client2 via relay"
	t.Logf("client2 → client1 via relay: %q", testMsg)

	response, err := client2.SendAndReceive(
		ctx,
		relay.Host.ID().String(),
		client1.Host.ID().String(),
		testMsg,
	)
	if err != nil {
		t.Fatalf("SendAndReceive failed: %v", err)
	}

	expected := fmt.Sprintf("echo: %s", testMsg)
	if response != expected {
		t.Errorf("unexpected response:\n  got  %q\n  want %q", response, expected)
	} else {
		t.Logf("✓ response: %q", response)
	}
}

// TestP2PViaRelay_MultiMessage verifies that the echo handler correctly handles
// multiple sequential messages on separate streams.
func TestP2PViaRelay_MultiMessage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	relay, err := raft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	defer relay.Host.Close()

	client1, err := raft.NewP2PNode(ctx, relay.Addrs[0], filepath.Join(tmpDir, "client1.key"))
	if err != nil {
		t.Fatalf("client1: %v", err)
	}
	defer client1.Close()
	client1.SetEchoHandler()

	client2, err := raft.NewP2PNode(ctx, relay.Addrs[0], filepath.Join(tmpDir, "client2.key"))
	if err != nil {
		t.Fatalf("client2: %v", err)
	}
	defer client2.Close()

	messages := []string{
		"ping 1",
		"ping 2",
		"ping 3",
	}

	for _, msg := range messages {
		t.Run(msg, func(t *testing.T) {
			resp, err := client2.SendAndReceive(
				ctx,
				relay.Host.ID().String(),
				client1.Host.ID().String(),
				msg,
			)
			if err != nil {
				t.Fatalf("SendAndReceive(%q): %v", msg, err)
			}
			want := "echo: " + msg
			if resp != want {
				t.Errorf("got %q, want %q", resp, want)
			}
			t.Logf("✓ %q → %q", msg, resp)
		})
	}
}
