package evm

import (
	"context"
	"errors"
	"log"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	ErrNoRPCURLs     = errors.New("no RPC URLs provided")
	ErrAllRPCsFailed = errors.New("all RPC URLs failed")
)

// RPCClient is the interface for EVM RPC calls. Client implements this.
type RPCClient interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	FilterLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []common.Address) ([]types.Log, error)
}

// DialFn creates an RPCClient from a URL. Injected for testing.
type DialFn func(url string) (RPCClient, error)

// DefaultDialFn connects to a real EVM RPC endpoint.
func DefaultDialFn(url string) (RPCClient, error) {
	return NewClient(url)
}

// RPCPool wraps multiple RPC URLs with automatic fallback.
// When the active client fails, it rotates to the next URL.
// Failed URLs are permanently removed (restart to reset).
type RPCPool struct {
	mu        sync.RWMutex
	active    RPCClient
	activeURL string
	remaining []string
	dialFn    DialFn
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
		dialFn: dialFn,
	}

	// Try connecting to each URL in order
	for i, url := range urls {
		client, err := dialFn(url)
		if err != nil {
			log.Printf("rpc pool: %s failed to connect: %v", url, err)
			continue
		}
		pool.active = client
		pool.activeURL = url
		pool.remaining = make([]string, len(urls[i+1:]))
		copy(pool.remaining, urls[i+1:])
		if i > 0 {
			log.Printf("rpc pool: connected to %s (primary %s failed)", url, urls[0])
		}
		return pool, nil
	}

	return nil, ErrAllRPCsFailed
}

// rotate switches to the next remaining URL. Returns error if none left.
func (p *RPCPool) rotate() error {
	// Already under write lock from caller
	for len(p.remaining) > 0 {
		url := p.remaining[0]
		p.remaining = p.remaining[1:]

		client, err := p.dialFn(url)
		if err != nil {
			log.Printf("rpc pool: %s failed: %v", url, err)
			continue
		}

		p.active = client
		p.activeURL = url
		log.Printf("rpc pool: switched to %s", url)
		return nil
	}

	return ErrAllRPCsFailed
}

// callWithFallback executes fn on the active client, rotating on failure.
func callWithFallback[T any](p *RPCPool, fn func(RPCClient) (T, error)) (T, error) {
	p.mu.RLock()
	client := p.active
	p.mu.RUnlock()

	result, err := fn(client)
	if err == nil {
		return result, nil
	}

	// Rotate through remaining URLs until one succeeds or all fail
	for {
		p.mu.Lock()
		rotateErr := p.rotate()
		if rotateErr != nil {
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
}

func (p *RPCPool) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return callWithFallback(p, func(c RPCClient) (*types.Header, error) {
		return c.HeaderByNumber(ctx, number)
	})
}

func (p *RPCPool) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return callWithFallback(p, func(c RPCClient) (*types.Block, error) {
		return c.BlockByNumber(ctx, number)
	})
}

func (p *RPCPool) FilterLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []common.Address) ([]types.Log, error) {
	return callWithFallback(p, func(c RPCClient) ([]types.Log, error) {
		return c.FilterLogs(ctx, fromBlock, toBlock, addresses)
	})
}

// ActiveURL returns the currently active RPC URL.
func (p *RPCPool) ActiveURL() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeURL
}
