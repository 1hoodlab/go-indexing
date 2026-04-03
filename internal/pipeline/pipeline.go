package pipeline

import (
	"context"
	"log"
	"sync"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

const defaultBufferSize = 1000

type Pipeline struct {
	subscriber core.Subscriber
	fetcher    core.LogFetcher
	registry   *core.Registry
	store      core.StorageInterface
	contracts  []core.ContractConfig
	chainName  string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	blockCh    chan core.BlockEvent
}

func New(
	chainName string,
	subscriber core.Subscriber,
	fetcher core.LogFetcher,
	registry *core.Registry,
	store core.StorageInterface,
	contracts []core.ContractConfig,
) *Pipeline {
	return &Pipeline{
		subscriber: subscriber,
		fetcher:    fetcher,
		registry:   registry,
		store:      store,
		contracts:  contracts,
		chainName:  chainName,
		blockCh:    make(chan core.BlockEvent, defaultBufferSize),
	}
}

func (p *Pipeline) Start() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	startBlock := uint64(0)
	if len(p.contracts) > 0 {
		startBlock = nextBlock(p.contracts[0])
		for _, c := range p.contracts[1:] {
			if nb := nextBlock(c); nb < startBlock {
				startBlock = nb
			}
		}
		log.Printf("[%s] starting from block %d (%d contracts)", p.chainName, startBlock, len(p.contracts))
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.blockCh)
		if err := p.subscriber.SubscribeBlocks(p.ctx, startBlock, p.blockCh); err != nil && p.ctx.Err() == nil {
			log.Printf("[%s] subscriber error: %v", p.chainName, err)
		}
	}()

	p.wg.Add(1)
	go p.processBlocks()

	return nil
}

func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func nextBlock(c core.ContractConfig) uint64 {
	if c.LastScanned == 0 {
		return c.StartBlock
	}
	return c.LastScanned + 1
}
