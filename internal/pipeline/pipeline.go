package pipeline

import (
	"context"
	"log"
	"sync"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage"
)

// Pipeline orchestrates block ingestion from chain to storage.
type Pipeline struct {
	cfg      *config.Config
	evm      *evm.Client
	store    *storage.MongoDB
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	blockCh  chan *evm.BlockEvent
}

// New creates a new pipeline.
func New(cfg *config.Config) (*Pipeline, error) {
	client, err := evm.NewClient(cfg.EVM.RPCURL)
	if err != nil {
		return nil, err
	}

	store, err := storage.NewMongoDB(&cfg.Storage)
	if err != nil {
		return nil, err
	}

	bufferSize := cfg.Pipeline.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	return &Pipeline{
		cfg:     cfg,
		evm:     client,
		store:   store,
		blockCh: make(chan *evm.BlockEvent, bufferSize),
	}, nil
}

// Start begins block subscription and processing.
func (p *Pipeline) Start() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	sub := evm.NewSubscriber(p.evm)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.blockCh)
		if err := sub.SubscribeBlocks(p.ctx, p.blockCh); err != nil && p.ctx.Err() == nil {
			log.Printf("subscriber error: %v", err)
		}
	}()

	// Process blocks
	p.wg.Add(1)
	go p.processBlocks()

	return nil
}

// Stop gracefully stops the pipeline.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *Pipeline) processBlocks() {
	defer p.wg.Done()

	for ev := range p.blockCh {
		if ev == nil {
			continue
		}
		// TODO: fetch full block, resolve labels, write to MongoDB
		_ = ev
		log.Printf("received block %s", ev.Number.String())
	}
}
