//go:build mage
// +build mage

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-pebble-raft/pkg/raft"
)

// Relay starts the libp2p Relay and Signaling server
func Relay(keyPath string) error {
	if keyPath == "" {
		keyPath = "relay.key"
	}
	fmt.Printf("Starting Relay server with key: %s...\n", keyPath)
	return raft.StartRelay(keyPath)
}

// Client starts a libp2p client connected to the specified relay
func Client(relayAddr string, targetPeerID string, keyPath string) error {
	if relayAddr == "" {
		return fmt.Errorf("relay address is required")
	}
	if keyPath == "" {
		keyPath = "client.key"
	}

	ctx := context.Background()
	node, err := raft.NewP2PNode(ctx, relayAddr, keyPath)
	if err != nil {
		return err
	}
	defer node.Close()

	fmt.Printf("Connected! ID: %s\n", node.Host.ID())
	fmt.Printf("My Address: %s\n", node.GetAddress())

	if targetPeerID != "" {
		return node.Chat(ctx, targetPeerID)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	return nil
}

// TestP2PRelay runs an end-to-end test entirely in-process:
//
//  1. Starts a relay node (ephemeral TCP port).
//  2. Connects client1 to the relay; client1 runs an echo handler.
//  3. Connects client2 to the relay.
//  4. client2 sends a message to client1 through the relay circuit.
//  5. Verifies the echoed response and prints PASS / FAIL.
//
// Run with: go run github.com/magefile/mage -v TestP2PRelay
func TestP2PRelay() error {
	const timeout = 90 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "p2p-relay-test-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("════════════════════════════════════════")
	fmt.Println(" libp2p P2P-via-Relay Integration Test")
	fmt.Println("════════════════════════════════════════")

	// ── Step 1: Relay ───────────────────────────────
	fmt.Println("\n[1/4] Starting relay node (port 0 = ephemeral)…")
	relay, err := raft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		return fmt.Errorf("relay start failed: %w", err)
	}
	defer relay.Host.Close()

	if len(relay.Addrs) == 0 {
		return fmt.Errorf("relay has no listen addresses")
	}
	relayAddr := relay.Addrs[0]
	relayID := relay.Host.ID().String()
	fmt.Printf("    relay addr : %s\n", relayAddr)

	// ── Step 2: Client1 (echo server) ───────────────
	fmt.Println("\n[2/4] Starting client1 (echo handler)…")
	client1, err := raft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "client1.key"))
	if err != nil {
		return fmt.Errorf("client1 start failed: %w", err)
	}
	defer client1.Close()
	client1.SetEchoHandler() // overrides the default print handler
	fmt.Printf("    client1 ID : %s\n", client1.Host.ID())

	// ── Step 3: Client2 (sender) ─────────────────────
	fmt.Println("\n[3/4] Starting client2 (sender)…")
	client2, err := raft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "client2.key"))
	if err != nil {
		return fmt.Errorf("client2 start failed: %w", err)
	}
	defer client2.Close()
	fmt.Printf("    client2 ID : %s\n", client2.Host.ID())

	// ── Step 4: Send message client2 → client1 ───────
	testMsg := "hello from client2 via relay"
	fmt.Printf("\n[4/4] client2 → client1 : %q\n", testMsg)

	response, err := client2.SendAndReceive(ctx, relayID, client1.Host.ID().String(), testMsg)
	if err != nil {
		fmt.Printf("\n✗ FAIL — SendAndReceive error: %v\n", err)
		return err
	}

	expected := "echo: " + testMsg
	if !strings.EqualFold(response, expected) {
		fmt.Printf("\n✗ FAIL\n  got : %q\n  want: %q\n", response, expected)
		return fmt.Errorf("unexpected response: got %q, want %q", response, expected)
	}

	fmt.Printf("\n✓ PASS — response: %q\n", response)
	fmt.Println("════════════════════════════════════════")
	return nil
}
