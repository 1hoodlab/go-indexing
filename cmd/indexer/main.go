package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/handlers"
	"github.com/1hoodlab/go-indexing.git/internal/pipeline"
)

func main() {
	p, err := pipeline.New()
	if err != nil {
		log.Fatalf("failed to create pipeline: %v", err)
	}

	// Register custom event handlers
	p.Registry().RegisterAll(
		handlers.Entry("MemberAdded(address,uint8,uint256,uint256)", handlers.MemberAdded(p.Store())),
	)

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
