package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// ──────────────────────────────────────────────────────────────
//  Connection wrapper
// ──────────────────────────────────────────────────────────────

// PoolConn wraps a gRPC ClientConn with metadata the pool needs.
type PoolConn struct {
	*grpc.ClientConn
	addr    string
	created time.Time
}

// ──────────────────────────────────────────────────────────────
//  Pool
// ──────────────────────────────────────────────────────────────

// ConnPool is a thread-safe pool of gRPC client connections to a single
// target address. It uses a buffered channel as the free-list and creates
// new connections on demand up to maxSize.
//
// Usage:
//
//	pool := NewConnPool("localhost:9000", 8, 5*time.Second)
//	conn, err := pool.Acquire(ctx)
//	// use conn…
//	pool.Release(conn)
//	pool.Close()
type ConnPool struct {
	addr        string
	maxSize     int
	dialTimeout time.Duration

	mu       sync.Mutex
	conns    chan *PoolConn // buffered channel acts as the free-list
	numOpen  int           // total connections created (in-use + idle)
	closed   bool

	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewConnPool creates a connection pool to addr with at most maxSize
// connections. dialTimeout controls how long Acquire waits for a new dial.
func NewConnPool(addr string, maxSize int, dialTimeout time.Duration) *ConnPool {
	if maxSize <= 0 {
		maxSize = 4
	}
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	p := &ConnPool{
		addr:        addr,
		maxSize:     maxSize,
		dialTimeout: dialTimeout,
		conns:       make(chan *PoolConn, maxSize),
		closeCh:     make(chan struct{}),
	}
	// Background health checker evicts dead connections.
	p.wg.Add(1)
	go p.healthCheck()
	return p
}

// Acquire returns an idle connection from the pool, or dials a new one if
// the pool is empty and hasn't reached maxSize. Blocks if the pool is
// exhausted, respecting the context deadline.
func (p *ConnPool) Acquire(ctx context.Context) (*PoolConn, error) {
	// Fast path: try to grab an idle connection without locking.
	select {
	case conn := <-p.conns:
		if isHealthy(conn) {
			return conn, nil
		}
		// Unhealthy — close it and fall through to create a new one.
		p.closeConn(conn)
	default:
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool: closed")
	}

	// Room to create a new connection?
	if p.numOpen < p.maxSize {
		p.numOpen++
		p.mu.Unlock()
		conn, err := p.dial(ctx)
		if err != nil {
			p.mu.Lock()
			p.numOpen--
			p.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}
	p.mu.Unlock()

	// Pool exhausted — wait for a release or context cancellation.
	select {
	case conn := <-p.conns:
		if isHealthy(conn) {
			return conn, nil
		}
		p.closeConn(conn)
		// Try dialing a replacement.
		return p.dial(ctx)
	case <-ctx.Done():
		return nil, fmt.Errorf("pool: acquire: %w", ctx.Err())
	}
}

// Release returns a connection to the pool. If the pool is full or the
// connection is unhealthy, it is closed instead.
func (p *ConnPool) Release(conn *PoolConn) {
	if conn == nil {
		return
	}
	if !isHealthy(conn) {
		p.closeConn(conn)
		return
	}
	select {
	case p.conns <- conn:
		// returned to pool
	default:
		// pool is full
		p.closeConn(conn)
	}
}

// Close drains the pool and closes all connections.
func (p *ConnPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.closeCh)
	p.wg.Wait()

	close(p.conns)
	for conn := range p.conns {
		conn.ClientConn.Close()
	}
}

// ──────────────────────────────────────────────────────────────
//  Internal
// ──────────────────────────────────────────────────────────────

func (p *ConnPool) dial(ctx context.Context) (*PoolConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, p.dialTimeout)
	defer cancel()

	cc, err := grpc.DialContext(dialCtx, p.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("pool: dial %q: %w", p.addr, err)
	}
	return &PoolConn{
		ClientConn: cc,
		addr:       p.addr,
		created:    time.Now(),
	}, nil
}

func (p *ConnPool) closeConn(conn *PoolConn) {
	conn.ClientConn.Close()
	p.mu.Lock()
	p.numOpen--
	p.mu.Unlock()
}

func isHealthy(conn *PoolConn) bool {
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// healthCheck periodically drains and re-tests idle connections, removing
// any that have become unhealthy.
func (p *ConnPool) healthCheck() {
	defer p.wg.Done()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.evictUnhealthy()
		}
	}
}

func (p *ConnPool) evictUnhealthy() {
	n := len(p.conns)
	for i := 0; i < n; i++ {
		select {
		case conn := <-p.conns:
			if isHealthy(conn) {
				// Put it back.
				select {
				case p.conns <- conn:
				default:
					p.closeConn(conn)
				}
			} else {
				p.closeConn(conn)
			}
		default:
			return
		}
	}
}
