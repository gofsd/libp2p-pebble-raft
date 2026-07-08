// Package rafttransport adapts a go-libp2p host into a hashicorp/raft
// StreamLayer, so raft RPCs travel over libp2p streams instead of raw TCP.
//
// raft.ServerAddress values used with this transport must be full libp2p
// multiaddrs that include a /p2p/<peer-id> component (e.g. what
// peer.AddrInfo.Addrs()/p2p-circuit resolves to). Callers are responsible for
// resolving a peer id to such an address (pkg/registry does this) before
// calling raft.AddVoter or otherwise referencing a ServerAddress.
package rafttransport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

// ProtocolID is the libp2p protocol used for raft RPC streams.
const ProtocolID = protocol.ID("/libp2p-pebble-raft/raft/1.0.0")

// streamLayer implements raft.StreamLayer over a libp2p host.
type streamLayer struct {
	host host.Host
	pid  protocol.ID

	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	incoming chan network.Stream
}

// NewStreamLayer registers a stream handler on h for the raft protocol and
// returns a raft.StreamLayer that Accept()s incoming raft streams and Dial()s
// outgoing ones.
func NewStreamLayer(h host.Host) raft.StreamLayer {
	sl := &streamLayer{
		host:     h,
		pid:      ProtocolID,
		closeCh:  make(chan struct{}),
		incoming: make(chan network.Stream, 16),
	}
	h.SetStreamHandler(sl.pid, func(s network.Stream) {
		select {
		case sl.incoming <- s:
		case <-sl.closeCh:
			s.Reset()
		}
	})
	return sl
}

// Accept implements net.Listener.
func (sl *streamLayer) Accept() (net.Conn, error) {
	select {
	case s := <-sl.incoming:
		return newConn(s), nil
	case <-sl.closeCh:
		return nil, fmt.Errorf("rafttransport: listener closed")
	}
}

// Close implements net.Listener.
func (sl *streamLayer) Close() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if sl.closed {
		return nil
	}
	sl.closed = true
	sl.host.RemoveStreamHandler(sl.pid)
	close(sl.closeCh)
	return nil
}

// Addr implements net.Listener.
func (sl *streamLayer) Addr() net.Addr {
	return peerAddr(sl.host.ID().String())
}

// Dial implements raft.StreamLayer. address must be a multiaddr string
// containing a /p2p/<peer-id> component.
func (sl *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	maddr, err := multiaddr.NewMultiaddr(string(address))
	if err != nil {
		return nil, fmt.Errorf("rafttransport: invalid address %q: %w", address, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("rafttransport: address %q missing peer id: %w", address, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if len(info.Addrs) > 0 {
		sl.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
	}
	if err := sl.host.Connect(ctx, *info); err != nil {
		return nil, fmt.Errorf("rafttransport: connect to %s: %w", info.ID, err)
	}

	s, err := sl.host.NewStream(network.WithAllowLimitedConn(ctx, "raft"), info.ID, sl.pid)
	if err != nil {
		return nil, fmt.Errorf("rafttransport: new stream to %s: %w", info.ID, err)
	}
	return newConn(s), nil
}

// peerAddr is a minimal net.Addr wrapping a libp2p peer id / multiaddr string.
type peerAddr string

func (a peerAddr) Network() string { return "libp2p" }
func (a peerAddr) String() string  { return string(a) }

// conn adapts a network.Stream to net.Conn.
type conn struct {
	network.Stream
}

func newConn(s network.Stream) *conn { return &conn{Stream: s} }

func (c *conn) LocalAddr() net.Addr {
	return peerAddr(c.Stream.Conn().LocalPeer().String())
}

func (c *conn) RemoteAddr() net.Addr {
	return peerAddr(c.Stream.Conn().RemotePeer().String())
}

// NewTransport builds a raft.NetworkTransport that runs over h using the
// raft protocol stream handler registered by NewStreamLayer.
func NewTransport(h host.Host, timeout time.Duration) *raft.NetworkTransport {
	return raft.NewNetworkTransport(NewStreamLayer(h), 4, timeout, nil)
}
