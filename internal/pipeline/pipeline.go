package pipeline

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/handlers"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/1hoodlab/go-indexing.git/internal/storage"
	"go.mongodb.org/mongo-driver/bson"
)

const defaultBufferSize = 1000

var ErrNoRPC = errors.New("network has no RPC URLs")

// Pipeline orchestrates block ingestion from chain to storage.
type Pipeline struct {
	store     *storage.MongoDB
	network   *models.Network
	contracts []*models.Contract
	evm       *evm.Client
	registry  *handlers.Registry
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	blockCh   chan *evm.BlockEvent
}

// Registry returns the handler registry for registering custom event handlers.
func (p *Pipeline) Registry() *handlers.Registry {
	return p.registry
}

// RegisterMemberAdded registers a handler for MemberAdded(address,uint8,uint256,uint256).
func (p *Pipeline) RegisterMemberAdded() {
	p.registry.Register("MemberAdded(address,uint8,uint256,uint256)", handlers.MemberAdded(p.store))
}

// New creates a new pipeline. Loads networks and contracts from MongoDB schema.
func New() (*Pipeline, error) {
	cfg := config.MongoDBConfig()
	store, err := storage.NewMongoDB(&cfg)
	if err != nil {
		return nil, err
	}

	if err := store.EnsureIndexes(context.Background()); err != nil {
		return nil, err
	}

	// Load first active network from schema
	var network models.Network
	err = store.Networks().FindOne(context.Background(), bson.M{"status": models.StatusActive}).Decode(&network)
	if err != nil {
		return nil, err
	}
	if len(network.RPCURLs) == 0 {
		return nil, ErrNoRPC
	}

	// Load active contracts for this network
	cursor, err := store.Contracts().Find(context.Background(), bson.M{
		"network_id": network.ID,
		"status":     models.StatusActive,
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var contracts []*models.Contract
	if err := cursor.All(context.Background(), &contracts); err != nil {
		return nil, err
	}
	if len(contracts) == 0 {
		return nil, errors.New("no active contracts to index")
	}

	client, err := evm.NewClient(network.RPCURLs[0])
	if err != nil {
		return nil, err
	}

	registry := handlers.NewRegistry(handlers.SaveToLogs(store))

	return &Pipeline{
		store:     store,
		network:   &network,
		contracts: contracts,
		evm:       client,
		registry:  registry,
		blockCh:   make(chan *evm.BlockEvent, defaultBufferSize),
	}, nil
}

// Start begins block subscription and processing.
func (p *Pipeline) Start() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	// Backfill from min(contract.NextBlock) if we have contracts
	startBlock := uint64(0)
	if len(p.contracts) > 0 {
		startBlock = p.contracts[0].NextBlock()
		for _, c := range p.contracts[1:] {
			if nb := c.NextBlock(); nb < startBlock {
				startBlock = nb
			}
		}
		log.Printf("[%s] starting from block %d (%d contracts)", p.network.Name, startBlock, len(p.contracts))
	}

	sub := evm.NewSubscriber(p.evm)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.blockCh)
		if err := sub.SubscribeBlocksFrom(p.ctx, startBlock, p.blockCh); err != nil && p.ctx.Err() == nil {
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

	ctx := context.Background()
	for ev := range p.blockCh {
		if ev == nil {
			continue
		}

		if err := p.processBlock(ctx, ev.Number); err != nil {
			log.Printf("[%s] process block %s error: %v", p.network.Name, ev.Number.String(), err)
			continue
		}

		log.Printf("[%s] indexed block %s", p.network.Name, ev.Number.String())
	}
}
