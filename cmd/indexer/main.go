package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/internal/handlers"
	"github.com/1hoodlab/go-indexing.git/internal/pipeline"
)

func main() {
	cfg := config.Load()

	switch cfg.ChainType {
	case "evm":
		runEVM(cfg)
	default:
		log.Fatalf("unsupported chain type: %s", cfg.ChainType)
	}
}

func runEVM(cfg config.Config) {
	store, err := evm.NewStorage(cfg.MongoURI, cfg.Database)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer store.Close(context.Background())

	if err := store.EnsureIndexes(context.Background()); err != nil {
		log.Fatalf("failed to ensure indexes: %v", err)
	}

	contracts, err := store.LoadContracts(context.Background())
	if err != nil {
		log.Fatalf("failed to load contracts: %v", err)
	}
	if len(contracts) == 0 {
		log.Fatal("no active contracts to index")
	}

	contractModels, err := store.LoadContractModels(context.Background())
	if err != nil {
		log.Fatalf("failed to load contract models: %v", err)
	}

	// Load network from DB (contains RPC URLs)
	network, err := store.LoadNetwork(context.Background())
	if err != nil {
		log.Fatalf("failed to load network: %v", err)
	}
	if len(network.RPCURLs) == 0 {
		log.Fatal("network has no RPC URLs configured in database")
	}

	networkID := network.ID.Hex()

	// Create RPC pool with fallback
	pool, err := evm.NewRPCPool(network.RPCURLs)
	if err != nil {
		log.Fatalf("failed to create RPC pool: %v", err)
	}

	subscriber := evm.NewSubscriber(pool, cfg.PollInterval)
	fetcher := evm.NewLogFetcher(pool, networkID, contractModels)
	parser := evm.NewEventParser()

	registry := core.NewRegistry(handlers.SaveToLogs(store))

	registry.Register(
		parser.EventKey("MemberAdded(address,uint8,uint256,uint256)"),
		handlers.MemberAdded(store),
	)

	p := pipeline.New(cfg.ChainName, subscriber, fetcher, registry, store, contracts)
	if err := p.Start(); err != nil {
		log.Fatalf("failed to start pipeline: %v", err)
	}
	defer p.Stop()

	log.Printf("[%s] blockchain indexer started (chain_type=%s, rpc=%s)", cfg.ChainName, cfg.ChainType, pool.ActiveURL())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
}
