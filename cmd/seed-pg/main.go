package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/chains/solana"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

type seedFile struct {
	Network   seedNetwork    `json:"network"`
	Contracts []seedContract `json:"contracts"`
}

type seedNetwork struct {
	Name      string   `json:"name"`
	ChainType string   `json:"chain_type"`
	ChainID   int64    `json:"chain_id"`
	RPCURLs   []string `json:"rpc_urls"`
	Type      string   `json:"type"`
	Status    string   `json:"status"`
}

type seedContract struct {
	Name       string          `json:"name"`
	Address    string          `json:"address"`
	ABI        json.RawMessage `json:"abi,omitempty"`
	IDL        json.RawMessage `json:"idl,omitempty"`
	Events     []string        `json:"events"`
	StartBlock uint64          `json:"start_block"`
}

func main() {
	configPath := flag.String("config", "", "path to seed JSON file")
	checkpointOverride := flag.Uint64("checkpoint", 0, "override checkpoint (0 = min start_block)")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("usage: go run ./cmd/seed-pg --config cmd/seed/data/base.json")
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	var seed seedFile
	if err := json.Unmarshal(data, &seed); err != nil {
		log.Fatalf("parse config: %v", err)
	}

	cfg := config.Load()
	ctx := context.Background()
	store, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer store.Close()

	status := seed.Network.Status
	if status == "" {
		status = "active"
	}
	networkType := seed.Network.Type
	if networkType == "" {
		networkType = "mainnet"
	}
	networkID, err := store.UpsertNetwork(ctx, postgres.Network{
		Name:        seed.Network.Name,
		ChainID:     seed.Network.ChainID,
		ChainType:   seed.Network.ChainType,
		NetworkType: networkType,
		RPCURLs:     seed.Network.RPCURLs,
		Status:      status,
	})
	if err != nil {
		log.Fatalf("upsert network: %v", err)
	}

	checkpoint := *checkpointOverride
	if checkpoint == 0 {
		checkpoint = minStartBlock(seed.Contracts)
	}

	if err := store.UpsertConfig(ctx, networkID, postgres.ConfigKeyScanEnabled, "true", "Stop/start switch"); err != nil {
		log.Fatalf("config scan_enabled: %v", err)
	}
	if err := store.UpsertConfig(ctx, networkID, postgres.ConfigKeyCheckpoint, strconv.FormatUint(checkpoint, 10), "Next block to scan"); err != nil {
		log.Fatalf("config checkpoint: %v", err)
	}
	if err := store.UpsertConfig(ctx, networkID, postgres.ConfigKeyConfirmationDepth, "12", "Confirmation depth"); err != nil {
		log.Fatalf("config confirmation_depth: %v", err)
	}

	isSolana := seed.Network.ChainType == "solana"

	for _, c := range seed.Contracts {
		addr := c.Address
		if !isSolana {
			addr = strings.ToLower(addr)
		}

		abi := c.ABI
		if isSolana && len(c.IDL) > 0 {
			abi = c.IDL
		}

		contractID, err := store.UpsertContract(ctx, networkID, c.Name, addr, abi)
		if err != nil {
			log.Fatalf("upsert contract %s: %v", addr, err)
		}

		if isSolana {
			var configs []postgres.SolanaEventConfigInput
			for _, eventName := range c.Events {
				configs = append(configs, postgres.SolanaEventConfigInput{
					EventName:     eventName,
					Discriminator: solana.ComputeDiscriminator(eventName),
				})
			}
			if err := store.BulkInsertSolanaEventConfigs(ctx, contractID, configs); err != nil {
				log.Fatalf("insert solana event configs %s: %v", addr, err)
			}
			log.Printf("seeded program %s with %d events", addr, len(configs))
		} else {
			var sigs []postgres.EventSignatureInput
			for _, eventSig := range c.Events {
				sigs = append(sigs, postgres.EventSignatureInput{
					EventName: utils.EventNameFromSignature(eventSig),
					Signature: eventSig,
					Topic0:    utils.EventTopic0(eventSig),
				})
			}
			if err := store.BulkInsertEventSignatures(ctx, contractID, sigs); err != nil {
				log.Fatalf("insert signatures %s: %v", addr, err)
			}
			log.Printf("seeded contract %s with %d events", addr, len(sigs))
		}
	}

	log.Printf("seed complete: network_id=%d checkpoint=%d", networkID, checkpoint)
}

func minStartBlock(contracts []seedContract) uint64 {
	var min uint64
	for i, c := range contracts {
		if i == 0 || c.StartBlock < min {
			min = c.StartBlock
		}
	}
	return min
}
