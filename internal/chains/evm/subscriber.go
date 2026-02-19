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
func (s *Subscriber) SubscribeBlocks(ctx context.Context, out chan<- *BlockEvent) error {
	// Poll-based subscription (replace with websocket subscription when supported)
	header, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}

	start := header.Number.Uint64()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			header, err := s.client.HeaderByNumber(ctx, big.NewInt(int64(start)))
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
				out <- &BlockEvent{Number: header.Number}
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
