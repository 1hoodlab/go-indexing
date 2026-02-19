package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/pipeline"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	p, err := pipeline.New(cfg)
	if err != nil {
		log.Fatalf("failed to create pipeline: %v", err)
	}

	if err := p.Start(); err != nil {
		log.Fatalf("failed to start pipeline: %v", err)
	}
	defer p.Stop()

	log.Println("blockchain indexer started")

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
}
