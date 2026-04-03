package pipeline

import (
	"context"
	"log"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

func (p *Pipeline) processBlocks() {
	defer p.wg.Done()

	for ev := range p.blockCh {
		if err := p.processBlock(p.ctx, ev); err != nil {
			log.Printf("[%s] process block %d error: %v", p.chainName, ev.Number, err)
			continue
		}
		log.Printf("[%s] indexed block %d", p.chainName, ev.Number)
	}
}

func (p *Pipeline) processBlock(ctx context.Context, block core.BlockEvent) error {
	events, err := p.fetcher.FetchLogs(ctx, block, p.contracts)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		batches := make(map[string][]core.RawEvent)
		for _, ev := range events {
			batches[ev.EventKey] = append(batches[ev.EventKey], ev)
		}

		for eventKey, batch := range batches {
			if err := p.registry.Dispatch(ctx, eventKey, batch); err != nil {
				log.Printf("[%s] handler error (key=%s): %v", p.chainName, eventKey, err)
				return err
			}
		}
	}

	for i := range p.contracts {
		nb := nextBlock(p.contracts[i])
		if block.Number >= nb {
			if err := p.store.UpdateScanProgress(ctx, p.contracts[i].Address, block.Number); err != nil {
				return err
			}
			p.contracts[i].LastScanned = block.Number
		}
	}

	return nil
}
