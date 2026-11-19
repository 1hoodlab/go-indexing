package solana

import (
	"context"
	"errors"
	"log"
	"sync"
)

var (
	ErrNoRPCURLs     = errors.New("solana: no RPC URLs provided")
	ErrAllRPCsFailed = errors.New("solana: all RPC URLs failed")
)

// SolanaRPCClient is the interface for Solana RPC calls.
type SolanaRPCClient interface {
	GetSlot(ctx context.Context) (uint64, error)
	GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error)
	GetBlock(ctx context.Context, slot uint64) (*Block, error)
}

type DialFn func(url string) (SolanaRPCClient, error)

func DefaultDialFn(url string) (SolanaRPCClient, error) {
	return NewClient(url)
}

type RPCPool struct {
	mu        sync.RWMutex
	allURLs   []string
	active    SolanaRPCClient
	activeURL string
	remaining []string
	dialFn    DialFn
}

func NewRPCPool(urls []string) (*RPCPool, error) {
	return NewRPCPoolWithDialFn(urls, DefaultDialFn)
}

func NewRPCPoolWithDialFn(urls []string, dialFn DialFn) (*RPCPool, error) {
	if len(urls) == 0 {
		return nil, ErrNoRPCURLs
	}
	pool := &RPCPool{
		allURLs: append([]string(nil), urls...),
		dialFn:  dialFn,
	}
	for i, url := range urls {
		client, err := dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed to connect: %v", url, err)
			continue
		}
		pool.active = client
		pool.activeURL = url
		pool.remaining = make([]string, len(urls[i+1:]))
		copy(pool.remaining, urls[i+1:])
		return pool, nil
	}
	return nil, ErrAllRPCsFailed
}

func (p *RPCPool) rotate() error {
	for len(p.remaining) > 0 {
		url := p.remaining[0]
		p.remaining = p.remaining[1:]
		client, err := p.dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed: %v", url, err)
			continue
		}
		p.active = client
		p.activeURL = url
		log.Printf("solana rpc pool: switched to %s", url)
		return nil
	}
	return ErrAllRPCsFailed
}

func (p *RPCPool) resetRemainingLocked() {
	if len(p.allURLs) <= 1 {
		p.remaining = nil
		return
	}
	p.remaining = make([]string, 0, len(p.allURLs)-1)
	for _, url := range p.allURLs {
		if url != p.activeURL {
			p.remaining = append(p.remaining, url)
		}
	}
}

func callWithFallback[T any](p *RPCPool, fn func(SolanaRPCClient) (T, error)) (T, error) {
	p.mu.Lock()
	p.resetRemainingLocked()
	client := p.active
	p.mu.Unlock()

	result, err := fn(client)
	if err == nil {
		return result, nil
	}

	for {
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

func (p *RPCPool) ActiveURL() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeURL
}
