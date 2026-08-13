package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
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
