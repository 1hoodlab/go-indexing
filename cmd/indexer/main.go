package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
	"github.com/1hoodlab/go-indexing.git/internal/chains/solana"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer store.Close()

	switch cfg.ChainType {
	case "solana":
		runSolana(ctx, cfg, store)
	default:
		runEVM(ctx, cfg, store)
	}
}

func runEVM(ctx context.Context, cfg config.Config, store *postgres.Store) {
	runtimeCfg, err := store.LoadIndexerRuntimeConfig(ctx)
	if err != nil {
		log.Fatalf("failed to load indexer config: %v", err)
	}
	if len(runtimeCfg.RPCURLs) == 0 {
		log.Fatal("network has no rpc_urls configured")
	}
	if len(runtimeCfg.ContractEvents) == 0 {
		log.Fatal("no active contracts with evm_event_signatures")
	}
	if !runtimeCfg.ScanEnabled {
		log.Printf("scan_enabled=false — indexer idle until enabled in indexer_config")
	}

	pool, err := evm.NewRPCPool(runtimeCfg.RPCURLs)
	if err != nil {
		log.Fatalf("failed to create RPC pool: %v", err)
	}

	indexer := evm.NewBlockIndexer(cfg, store, runtimeCfg, pool)
	log.Printf("[%s] evm indexer started (rpc=%s, checkpoint=%d)", cfg.ChainName, pool.ActiveURL(), runtimeCfg.Checkpoint)
	if err := indexer.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("indexer error: %v", err)
	}
}

func runSolana(ctx context.Context, cfg config.Config, store *postgres.Store) {
	runtimeCfg, err := store.LoadSolanaRuntimeConfig(ctx)
	if err != nil {
		log.Fatalf("failed to load solana config: %v", err)
	}
	if len(runtimeCfg.RPCURLs) == 0 {
		log.Fatal("network has no rpc_urls configured")
	}
	if len(runtimeCfg.EventConfigs) == 0 {
		log.Fatal("no active programs with solana_event_configs")
	}
	if !runtimeCfg.ScanEnabled {
		log.Printf("scan_enabled=false — indexer idle until enabled in indexer_config")
	}

	pool, err := solana.NewRPCPool(runtimeCfg.RPCURLs)
	if err != nil {
		log.Fatalf("failed to create Solana RPC pool: %v", err)
	}

	indexer := solana.NewSlotIndexer(cfg, store, runtimeCfg, pool)
	log.Printf("[%s] solana indexer started (rpc=%s, checkpoint=%d)", cfg.ChainName, pool.ActiveURL(), runtimeCfg.Checkpoint)
	if err := indexer.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("indexer error: %v", err)
	}
}
