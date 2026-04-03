package evm

import (
	"context"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

type Subscriber struct {
	client       RPCClient
	pollInterval time.Duration
}

func NewSubscriber(client RPCClient, pollInterval time.Duration) *Subscriber {
	return &Subscriber{client: client, pollInterval: pollInterval}
}

func (s *Subscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	header, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}
	latest := header.Number.Uint64()
	start := from
	if from == 0 {
		start = latest
	}

	// Backfill
	for start <= latest {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		out <- core.BlockEvent{Number: start}
		start++
	}

	// Live poll
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			header, err = s.client.HeaderByNumber(ctx, nil)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "not found") {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(s.pollInterval):
					}
					continue
				}
				return err
			}
			latest = header.Number.Uint64()
			for start <= latest {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				out <- core.BlockEvent{Number: start}
				start++
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
		}
	}
}
