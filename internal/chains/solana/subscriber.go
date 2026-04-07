package solana

import (
	"context"
	"log"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

const backfillBatchSize = uint64(100)

// Subscriber implements core.Subscriber for Solana via HTTP polling.
type Subscriber struct {
	client       SolanaRPCClient
	pollInterval time.Duration
}

func NewSubscriber(client SolanaRPCClient, pollInterval time.Duration) *Subscriber {
	return &Subscriber{client: client, pollInterval: pollInterval}
}

func (s *Subscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	lastProcessed := from - 1
	if from == 0 {
		slot, err := s.client.GetSlot(ctx)
		if err != nil {
			return err
		}
		lastProcessed = slot
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentSlot, err := s.client.GetSlot(ctx)
		if err != nil {
			log.Printf("solana subscriber: getSlot error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		if currentSlot <= lastProcessed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		startSlot := lastProcessed + 1
		limit := currentSlot - lastProcessed
		if limit > backfillBatchSize {
			limit = backfillBatchSize
		}

		confirmedSlots, err := s.client.GetBlocksWithLimit(ctx, startSlot, limit)
		if err != nil {
			log.Printf("solana subscriber: getBlocksWithLimit error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		for _, slot := range confirmedSlots {
			block, err := s.client.GetBlock(ctx, slot)
			if err != nil {
				log.Printf("solana subscriber: getBlock(%d) error: %v", slot, err)
				continue
			}
			if block == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- core.BlockEvent{
				Number:    slot,
				Hash:      block.Blockhash,
				Timestamp: uint64(block.BlockTime),
			}:
			}
			lastProcessed = slot
		}

		if len(confirmedSlots) == 0 {
			lastProcessed = currentSlot
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}
}
