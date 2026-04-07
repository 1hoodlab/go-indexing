package solana

import (
	"context"
	"errors"
	"log"
	"sync"
)

var (
	ErrNoRPCURLs     = errors.New("no RPC URLs provided")
	ErrAllRPCsFailed = errors.New("all RPC URLs failed")
)

// SolanaRPCClient is the interface for Solana RPC calls.
type SolanaRPCClient interface {
	GetSlot(ctx context.Context) (uint64, error)
	GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error)
	GetBlock(ctx context.Context, slot uint64) (*Block, error)
}

// DialFn creates a SolanaRPCClient from a URL. Injected for testing.
type DialFn func(url string) (SolanaRPCClient, error)

// DefaultDialFn connects to a real Solana RPC endpoint.
func DefaultDialFn(url string) (SolanaRPCClient, error) {
	return NewClient(url), nil
}

// RPCPool wraps multiple Solana RPC URLs with automatic fallback.
// When the active client fails, it rotates to the next URL.
// Failed URLs are permanently removed (restart to reset).
type RPCPool struct {
	mu          sync.RWMutex
	active      SolanaRPCClient
	activeURL   string
	activeIndex int
	urls        []string
	dialFn      DialFn
}

// NewRPCPool creates a pool from a list of RPC URLs.
func NewRPCPool(urls []string) (*RPCPool, error) {
	return NewRPCPoolWithDialFn(urls, DefaultDialFn)
}

// NewRPCPoolWithDialFn creates a pool with a custom dial function (for testing).
func NewRPCPoolWithDialFn(urls []string, dialFn DialFn) (*RPCPool, error) {
	if len(urls) == 0 {
		return nil, ErrNoRPCURLs
	}
	pool := &RPCPool{
		dialFn:      dialFn,
		urls:        append([]string(nil), urls...),
		activeIndex: -1,
	}
	for i, url := range urls {
		client, err := dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed to connect: %v", url, err)
			continue
		}
		pool.active = client
		pool.activeURL = url
		pool.activeIndex = i
		if i > 0 {
			log.Printf("solana rpc pool: connected to %s (primary %s failed)", url, urls[0])
		}
		return pool, nil
	}
	return nil, ErrAllRPCsFailed
}

// rotate cycles to the next URL and reconnects. It never removes URLs permanently.
// Caller must hold the write lock.
func (p *RPCPool) rotate() error {
	if len(p.urls) == 0 {
		return ErrNoRPCURLs
	}
	start := p.activeIndex
	for tries := 0; tries < len(p.urls)-1; tries++ {
		next := (start + 1 + tries) % len(p.urls)
		url := p.urls[next]
		client, err := p.dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed: %v", url, err)
			continue
		}
		p.active = client
		p.activeURL = url
		p.activeIndex = next
		log.Printf("solana rpc pool: switched to %s", url)
		return nil
	}
	return ErrAllRPCsFailed
}

// callWithFallback executes fn on the active client, rotating on failure.
func callWithFallback[T any](p *RPCPool, fn func(SolanaRPCClient) (T, error)) (T, error) {
	p.mu.RLock()
	client := p.active
	p.mu.RUnlock()

	result, err := fn(client)
	if err == nil {
		return result, nil
	}

	for attempts := 0; attempts < len(p.urls)-1; attempts++ {
		p.mu.Lock()
		if rotateErr := p.rotate(); rotateErr != nil {
			p.mu.Unlock()
			var zero T
			return zero, ErrAllRPCsFailed
		}
		client = p.active
		p.mu.Unlock()

		result, err = fn(client)
		if err == nil {
			return result, nil
		}
	}
	var zero T
	return zero, ErrAllRPCsFailed
}

func (p *RPCPool) GetSlot(ctx context.Context) (uint64, error) {
	return callWithFallback(p, func(c SolanaRPCClient) (uint64, error) {
		return c.GetSlot(ctx)
	})
}

func (p *RPCPool) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	return callWithFallback(p, func(c SolanaRPCClient) ([]uint64, error) {
		return c.GetBlocksWithLimit(ctx, startSlot, limit)
	})
}

func (p *RPCPool) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	return callWithFallback(p, func(c SolanaRPCClient) (*Block, error) {
		return c.GetBlock(ctx, slot)
	})
}

// ActiveURL returns the currently active RPC URL.
func (p *RPCPool) ActiveURL() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeURL
}
