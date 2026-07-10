package raft

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	peerstore "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	libp2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	v2relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	_ "github.com/libp2p/go-libp2p/p2p/transport/webrtc"
	"github.com/multiformats/go-multiaddr"
)

const ProtocolID = "/libp2p-kv-raft/1.0.0"

// P2PNode represents a libp2p node that works behind strict NATs
type P2PNode struct {
	Host host.Host
}

// RelayNode wraps a relay host and its advertised addresses
type RelayNode struct {
	Host  host.Host
	Addrs []string // full multiaddrs including /p2p/<peerID>
}

// StartRelayNode starts a non-blocking relay node. port=0 picks an ephemeral TCP port.
// The node is shut down when ctx is cancelled.
func StartRelayNode(ctx context.Context, keyPath string, port int) (*RelayNode, error) {
	priv, err := LoadOrGenerateKey(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load relay identity: %w", err)
	}

	// Build relay resources starting from safe defaults, then override only what we need.
	// IMPORTANT: WithResources replaces the entire struct, so unset int fields become 0.
	// MaxReservationsPerIP=0 means "0 per IP", which refuses every reservation.
	// Always start from DefaultResources() to preserve those per-IP/ASN limits.
	rc := v2relay.DefaultResources()
	rc.Limit = &v2relay.RelayLimit{
		Duration: 1 * time.Hour,
		Data:     1 << 30, // 1 GB
	}
	rc.ReservationTTL = time.Hour
	rc.MaxReservations = 256
	rc.MaxCircuits = 256
	rc.BufferSize = 4096

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)),
		libp2p.DefaultTransports,
		libp2p.ForceReachabilityPublic(),
		libp2p.EnableRelayService(v2relay.WithResources(rc)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create relay host: %w", err)
	}

	go func() {
		<-ctx.Done()
		h.Close()
	}()

	var addrs []string
	for _, addr := range h.Addrs() {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", addr, h.ID()))
	}

	fmt.Printf("[Relay] ID:  %s\n", h.ID())
	for _, a := range addrs {
		fmt.Printf("[Relay] Addr: %s\n", a)
	}

	return &RelayNode{Host: h, Addrs: addrs}, nil
}

// LoadOrGenerateKey loads a private key from a file or generates a new one
func LoadOrGenerateKey(keyPath string) (crypto.PrivKey, error) {
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		if err != nil {
			return nil, err
		}
		data, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(data)), 0600); err != nil {
			return nil, err
		}
		return priv, nil
	}

	dataHex, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	data, err := hex.DecodeString(string(dataHex))
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalPrivateKey(data)
}

// NewP2PNode initializes a new node and connects it to a relay
func NewP2PNode(ctx context.Context, relayAddrStr string, keyPath string) (*P2PNode, error) {
	// 1. Load or generate identity
	priv, err := LoadOrGenerateKey(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load identity: %w", err)
	}

	// 2. Parse relay address
	relayAddr, err := multiaddr.NewMultiaddr(relayAddrStr)
	if err != nil {
		return nil, fmt.Errorf("invalid relay address: %w", err)
	}

	relayInfo, err := peerstore.AddrInfoFromP2pAddr(relayAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to get relay info: %w", err)
	}

	// 3. Create libp2p host
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip4/0.0.0.0/udp/0/webrtc-direct",
			"/p2p-circuit",
		),
		libp2p.DefaultTransports,
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	// 4. Connect to relay
	if err := h.Connect(ctx, *relayInfo); err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to connect to relay: %w", err)
	}

	// Wait for reservation
	fmt.Printf("Connected to relay %s, requesting reservation...\n", relayInfo.ID)
	_, err = relayclient.Reserve(ctx, h, *relayInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve relay: %w", err)
	}

	ctxWait, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	reserved := false
	for !reserved {
		select {
		case <-ctxWait.Done():
			fmt.Println("Warning: timed out waiting for relay reservation to appear in Addrs")
			reserved = true
		case <-time.After(500 * time.Millisecond):
			for _, addr := range h.Addrs() {
				if _, err := addr.ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
					fmt.Printf("Relay reservation successful: %s\n", addr)
					reserved = true
					break
				}
			}
		}
	}

	// Set stream handler for incoming messages
	h.SetStreamHandler(protocol.ID(ProtocolID), func(s network.Stream) {
		// Don't close immediately if we want to read
		fmt.Printf("\n[New Stream from %s]\n", s.Conn().RemotePeer())
		go func() {
			defer s.Close()
			scanner := bufio.NewScanner(s)
			for scanner.Scan() {
				fmt.Printf("\a\n[Message]: %s\n> ", scanner.Text())
			}
		}()
	})

	// Keep the reservation alive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				h.Connect(ctx, *relayInfo)
			}
		}
	}()

	return &P2PNode{Host: h}, nil
}

// Chat opens a stream to a target peer and starts the IO loop
func (n *P2PNode) Chat(ctx context.Context, targetPeerID string) error {
	pid, err := peerstore.Decode(targetPeerID)
	if err != nil {
		return fmt.Errorf("invalid target peer id: %w", err)
	}

	// 1. Get relay ID from our own connections
	var relayID peerstore.ID
	for _, con := range n.Host.Network().Conns() {
		relayID = con.RemotePeer()
		break
	}

	if relayID == "" {
		return fmt.Errorf("not connected to any relay")
	}

	// 2. Construct the Relay v2 circuit address: /p2p/RELAY_ID/p2p-circuit/p2p/TARGET_ID
	targetAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", relayID, targetPeerID))
	n.Host.Peerstore().AddAddr(pid, targetAddr, time.Hour)

	fmt.Printf("Dialing %s via relay %s...\n", pid, relayID)
	s, err := n.Host.NewStream(ctx, pid, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer s.Close()

	fmt.Printf("Connected to %s! Type your messages below.\n", pid)

	errCh := make(chan error, 1)

	// Outgoing
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("> ")
		for scanner.Scan() {
			text := scanner.Text()
			if _, err := s.Write([]byte(text + "\n")); err != nil {
				errCh <- err
				return
			}
			fmt.Print("> ")
		}
	}()

	// Incoming
	go func() {
		scanner := bufio.NewScanner(s)
		for scanner.Scan() {
			fmt.Printf("\r[Message]: %s\n> ", scanner.Text())
		}
		errCh <- io.EOF
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err == io.EOF {
			fmt.Println("\nPeer closed connection.")
			return nil
		}
		return err
	}
}

// SetEchoHandler installs a stream handler that echoes each received line back
// as "echo: <line>". Used by the listener node in tests.
func (n *P2PNode) SetEchoHandler() {
	n.Host.SetStreamHandler(protocol.ID(ProtocolID), func(s network.Stream) {
		defer s.Close()
		scanner := bufio.NewScanner(s)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[%s] received: %q\n", n.Host.ID().ShortString(), line)
			if _, err := fmt.Fprintf(s, "echo: %s\n", line); err != nil {
				return
			}
		}
	})
}

// SendAndReceive opens a stream to targetPeerID via the relay, sends msg, and
// returns the first response line. It dials through the relay using a circuit
// address, so direct connectivity is not required.
func (n *P2PNode) SendAndReceive(ctx context.Context, relayID string, targetPeerID string, msg string) (string, error) {
	pid, err := peerstore.Decode(targetPeerID)
	if err != nil {
		return "", fmt.Errorf("invalid target peer id: %w", err)
	}

	// /p2p/<relay>/p2p-circuit is the circuit transport address for targetPeerID.
	// The dialer will route the connection through the relay.
	circuitAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/p2p/%s/p2p-circuit", relayID))
	if err != nil {
		return "", fmt.Errorf("failed to build circuit addr: %w", err)
	}
	n.Host.Peerstore().AddAddr(pid, circuitAddr, time.Hour)

	fmt.Printf("[%s] dialing %s via relay %s...\n",
		n.Host.ID().ShortString(), pid.ShortString(), relayID[:8])

	// WithAllowLimitedConn tells the swarm to use the relay (limited/transient)
	// connection directly, instead of blocking in waitForDirectConn waiting for
	// a hole-punch upgrade that will never come in a loopback test environment.
	streamCtx := network.WithAllowLimitedConn(ctx, "relay circuit")
	s, err := n.Host.NewStream(streamCtx, pid, protocol.ID(ProtocolID))
	if err != nil {
		return "", fmt.Errorf("failed to open stream to %s: %w", pid.ShortString(), err)
	}
	defer s.Close()

	// Write message then close the write side so the echo handler sees EOF
	if _, err := fmt.Fprintf(s, "%s\n", msg); err != nil {
		return "", fmt.Errorf("failed to send message: %w", err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("failed to close write: %w", err)
	}

	// Read the single echoed response line
	scanner := bufio.NewScanner(s)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read error: %w", err)
	}
	return "", fmt.Errorf("no response received from %s", pid.ShortString())
}

// StartRelay initializes a relay and signaling server with Kademlia DHT
func StartRelay(keyPath string) error {
	priv, err := LoadOrGenerateKey(keyPath)
	if err != nil {
		return fmt.Errorf("failed to load identity: %w", err)
	}

	var kdht *dht.IpfsDHT
	// Create connection manager
	cm, err := libp2pconnmgr.NewConnManager(
		100, // low water mark
		400, // high water mark
		libp2pconnmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		return err
	}

	rc := v2relay.DefaultResources()
	rc.Limit = &v2relay.RelayLimit{
		Duration: 1 * time.Hour,
		Data:     1 << 30, // 1 GB
	}
	rc.ReservationTTL = time.Hour
	rc.MaxReservations = 1000
	rc.MaxCircuits = 1000
	rc.BufferSize = 2048

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip4/0.0.0.0/tcp/4002/ws",
			"/ip4/0.0.0.0/udp/4003/webrtc-direct",
		),
		libp2p.DefaultTransports,
		libp2p.EnableRelayService(v2relay.WithResources(rc)),
		libp2p.EnableHolePunching(),
		libp2p.ConnectionManager(cm),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			kdht, err = dht.New(context.Background(), h, dht.Mode(dht.ModeServer))
			return kdht, err
		}),
	)
	if err != nil {
		return err
	}
	defer h.Close()

	if err = kdht.Bootstrap(context.Background()); err != nil {
		return err
	}

	fmt.Printf("Relay Server with DHT started! ID: %s\n", h.ID())
	fmt.Println("Supported Protocols:")
	for _, proto := range h.Mux().Protocols() {
		fmt.Printf("  - %s\n", proto)
	}
	for _, addr := range h.Addrs() {
		fmt.Printf("  %s/p2p/%s\n", addr, h.ID())
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	return nil
}

// GetAddress returns the circuit address for this node
func (n *P2PNode) GetAddress() string {
	if len(n.Host.Addrs()) > 0 {
		return fmt.Sprintf("%s/p2p/%s", n.Host.Addrs()[0], n.Host.ID())
	}
	return ""
}

// Close stops the node
func (n *P2PNode) Close() error {
	return n.Host.Close()
}
