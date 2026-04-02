package evm

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"time"
)

// BlockEvent represents a new block to be processed.
type BlockEvent struct {
	Number *big.Int
}

// Subscriber subscribes to new blocks from an EVM chain.
type Subscriber struct {
	client *Client
}

// NewSubscriber creates a new block subscriber.
func NewSubscriber(client *Client) *Subscriber {
	return &Subscriber{client: client}
}

// PollInterval is the delay between RPC polls when waiting for next block.
const PollInterval = 12 * time.Second

// SubscribeBlocks subscribes to new blocks and sends them to the given channel.
// Uses startBlock if > 0; otherwise starts from latest. Backfills from startBlock to latest first.
func (s *Subscriber) SubscribeBlocks(ctx context.Context, out chan<- *BlockEvent) error {
	return s.SubscribeBlocksFrom(ctx, 0, out)
}

// SubscribeBlocksFrom subscribes from startBlock (backfill to latest, then poll for new).
func (s *Subscriber) SubscribeBlocksFrom(ctx context.Context, startBlock uint64, out chan<- *BlockEvent) error {
	header, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}

	latest := header.Number.Uint64()
	start := startBlock
	if startBlock == 0 {
		start = latest
	}

	// Backfill: emit blocks from start to latest (inclusive)
	for start <= latest {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		out <- &BlockEvent{Number: new(big.Int).SetUint64(start)}
		start++
	}

	// Live: poll for new blocks
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			header, err = s.client.HeaderByNumber(ctx, new(big.Int).SetUint64(start))
			if err != nil {
				// Block not mined yet ("not found") — wait and retry
				if strings.Contains(strings.ToLower(err.Error()), "not found") {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(PollInterval):
					}
					continue
				}
				return err
			}
			if header != nil {
				out <- &BlockEvent{Number: new(big.Int).SetUint64(start)}
				start++
			}
		}
	}
}

// RunWithCancel runs SubscribeBlocks with a cancellable context.
func (s *Subscriber) RunWithCancel(ctx context.Context, out chan<- *BlockEvent, wg *sync.WaitGroup) error {
	defer wg.Done()
	return s.SubscribeBlocks(ctx, out)
}
