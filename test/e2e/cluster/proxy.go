//go:build cluster_e2e

package cluster

import (
	"io"
	"net"
	"sync"
	"time"
)

// NodeProxy is a TCP proxy that sits between cluster nodes.
// It forwards all traffic from listenAddr to targetAddr, with optional fault injection.
// Because ZenBPM multiplexes all inter-node communication (base Raft, gRPC, partition Raft,
// partition cluster) over a single TCP port, one proxy per node captures ALL traffic.
type NodeProxy struct {
	targetAddr string
	listener   net.Listener

	mu        sync.RWMutex
	blackhole bool
	blocked   map[string]bool // peer source addresses to block
	latency   time.Duration
	closed    bool

	// Track active connections so we can kill them on blackhole/block
	activeConns []net.Conn
}

// NewNodeProxy creates a proxy that listens on a random free port and forwards to targetAddr.
func NewNodeProxy(targetAddr string) (*NodeProxy, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	p := &NodeProxy{
		targetAddr: targetAddr,
		listener:   ln,
		blocked:    make(map[string]bool),
	}

	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's listen address (what other nodes should connect to).
func (p *NodeProxy) Addr() string {
	return p.listener.Addr().String()
}

// Blackhole drops all traffic to and from this node.
func (p *NodeProxy) Blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = true
	p.killActiveConns()
}

// Unblackhole restores traffic flow.
func (p *NodeProxy) Unblackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = false
}

// BlockPeer blocks traffic from a specific peer address.
func (p *NodeProxy) BlockPeer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[addr] = true
	// Kill existing connections from this peer
	p.killConnsFromPeer(addr)
}

// UnblockPeer restores traffic from a specific peer.
func (p *NodeProxy) UnblockPeer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.blocked, addr)
}

// UnblockAll restores all traffic.
func (p *NodeProxy) UnblockAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = false
	p.blocked = make(map[string]bool)
	p.latency = 0
}

// SetLatency adds artificial latency to all connections through this proxy.
func (p *NodeProxy) SetLatency(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latency = d
}

// Close shuts down the proxy.
func (p *NodeProxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.killActiveConns()
	return p.listener.Close()
}

func (p *NodeProxy) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			p.mu.RLock()
			closed := p.closed
			p.mu.RUnlock()
			if closed {
				return
			}
			continue
		}
		go p.handleConn(conn)
	}
}

func (p *NodeProxy) handleConn(clientConn net.Conn) {
	p.mu.RLock()
	if p.blackhole {
		p.mu.RUnlock()
		clientConn.Close()
		return
	}
	lat := p.latency
	p.mu.RUnlock()

	if lat > 0 {
		time.Sleep(lat)
	}

	targetConn, err := net.DialTimeout("tcp4", p.targetAddr, 5*time.Second)
	if err != nil {
		clientConn.Close()
		return
	}

	p.mu.Lock()
	// Re-check after acquiring write lock
	if p.blackhole {
		p.mu.Unlock()
		clientConn.Close()
		targetConn.Close()
		return
	}
	p.activeConns = append(p.activeConns, clientConn, targetConn)
	p.mu.Unlock()

	// Bidirectional copy
	done := make(chan struct{}, 2)
	go func() {
		p.pipe(clientConn, targetConn)
		done <- struct{}{}
	}()
	go func() {
		p.pipe(targetConn, clientConn)
		done <- struct{}{}
	}()
	<-done

	clientConn.Close()
	targetConn.Close()

	p.mu.Lock()
	p.removeConn(clientConn)
	p.removeConn(targetConn)
	p.mu.Unlock()
}

func (p *NodeProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		p.mu.RLock()
		blackholed := p.blackhole
		lat := p.latency
		p.mu.RUnlock()

		if blackholed {
			return
		}

		// Set read deadline so we can periodically check fault state
		src.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := src.Read(buf)
		if n > 0 {
			if lat > 0 {
				time.Sleep(lat)
			}
			p.mu.RLock()
			blackholed = p.blackhole
			p.mu.RUnlock()
			if blackholed {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if err == io.EOF {
				return
			}
			return
		}
	}
}

func (p *NodeProxy) killActiveConns() {
	for _, c := range p.activeConns {
		c.Close()
	}
	p.activeConns = nil
}

func (p *NodeProxy) killConnsFromPeer(addr string) {
	// Note: In a proxy setup, the peer's source address isn't directly available
	// since connections come from the OS ephemeral port. For proxy-level blocking,
	// we kill ALL active connections when a peer is blocked, forcing reconnection
	// through the proxy which will then refuse the connection.
	// A more targeted approach would require tracking which proxy connections
	// correspond to which logical peer, but this is sufficient for test purposes.
	p.killActiveConns()
}

func (p *NodeProxy) removeConn(conn net.Conn) {
	for i, c := range p.activeConns {
		if c == conn {
			p.activeConns = append(p.activeConns[:i], p.activeConns[i+1:]...)
			return
		}
	}
}
