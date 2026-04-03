package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	Address    string          `json:"address"`
	ABI        json.RawMessage `json:"abi,omitempty"`
	IDL        string          `json:"idl,omitempty"`
	Events     []string        `json:"events"`
	StartBlock uint64          `json:"start_block"`
}

func main() {
	configPath := flag.String("config", "", "path to seed JSON file (e.g. data/sepolia.json)")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("usage: go run cmd/seed/main.go --config data/sepolia.json")
	}

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}

	var seed seedFile
	if err := json.Unmarshal(data, &seed); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	cfg := config.Load()
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database(cfg.Database)
	ctx := context.Background()
	now := time.Now()

	// Upsert network
	networkColl := db.Collection("networks")
	networkFilter := bson.M{"chain_id": seed.Network.ChainID}
	networkUpdate := bson.M{
		"$set": bson.M{
			"name":       seed.Network.Name,
			"chain_type": seed.Network.ChainType,
			"type":       models.NetworkType(seed.Network.Type),
			"rpc_urls":   seed.Network.RPCURLs,
			"status":     models.NetworkStatus(seed.Network.Status),
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	_, err = networkColl.UpdateOne(ctx, networkFilter, networkUpdate, options.Update().SetUpsert(true))
	if err != nil {
		log.Fatalf("failed to upsert network: %v", err)
	}
	log.Printf("upserted network: %s (chain_id: %d)", seed.Network.Name, seed.Network.ChainID)

	var net models.Network
	if err := networkColl.FindOne(ctx, networkFilter).Decode(&net); err != nil {
		log.Fatalf("failed to find network: %v", err)
	}

	// Upsert contracts
	contractColl := db.Collection("contracts")
	for _, sc := range seed.Contracts {
		addr := strings.ToLower(sc.Address)
		filter := bson.M{"network_id": net.ID, "address": addr}

		setFields := bson.M{
			"events":     sc.Events,
			"status":     models.StatusActive,
			"updated_at": now,
		}

		if len(sc.ABI) > 0 {
			// Compact ABI JSON
			var compact interface{}
			if err := json.Unmarshal(sc.ABI, &compact); err == nil {
				minified, _ := json.Marshal(compact)
				setFields["abi"] = string(minified)
			}
		}
		if sc.IDL != "" {
			setFields["idl"] = sc.IDL
		}

		update := bson.M{
			"$set": setFields,
			"$setOnInsert": bson.M{
				"start_block":        sc.StartBlock,
				"last_scanned_block": uint64(0),
				"created_at":         now,
			},
		}

		result, err := contractColl.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			log.Fatalf("failed to upsert contract %s: %v", addr, err)
		}

		if result.UpsertedCount > 0 {
			log.Printf("created contract: %s (start_block: %d)", addr, sc.StartBlock)
		} else if result.ModifiedCount > 0 {
			log.Printf("updated contract: %s", addr)
		} else {
			log.Printf("contract unchanged: %s", addr)
		}
	}

	log.Println("seed completed successfully")
}
