package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/1hoodlab/go-indexing.git/internal/storage"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	sepoliaChainID  = 11155111
	contractAddress = "0xF2363E51A4Bb4872F38576508256bBbcfABB7348"
	// Contract deployed at block 10293841 (from Sepolia Etherscan)
	contractStartBlock = 10293841
)

var sepoliaRPCURLs = []string{
	"https://ethereum-sepolia-rpc.publicnode.com", // PublicNode - free, no API key
	"https://rpc.sepolia.org",
	"https://rpc2.sepolia.org",
	"https://rpc.sepolia.online",
	"https://rpc-sepolia.rockx.com",
	"https://rpc.bordel.wtf/sepolia",
}

var contractABIRaw = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"by","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"EmergencyPaused","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"by","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"EmergencyUnpaused","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"FundsDeposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"FundsWithdrawn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"member","type":"address"},{"indexed":false,"internalType":"uint8","name":"role","type":"uint8"},{"indexed":false,"internalType":"uint256","name":"votingPower","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"MemberAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"member","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"MemberRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"member","type":"address"},{"indexed":false,"internalType":"uint8","name":"oldRole","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"newRole","type":"uint8"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"MemberRoleChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":true,"internalType":"address","name":"cancelledBy","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"ProposalCancelled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":true,"internalType":"address","name":"proposer","type":"address"},{"indexed":false,"internalType":"string","name":"title","type":"string"},{"indexed":false,"internalType":"uint256","name":"startTime","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"endTime","type":"uint256"}],"name":"ProposalCreated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":true,"internalType":"address","name":"executor","type":"address"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"ProposalExecuted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"totalVotes","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"QuorumReached","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"proposalId","type":"uint256"},{"indexed":true,"internalType":"address","name":"voter","type":"address"},{"indexed":false,"internalType":"uint8","name":"voteType","type":"uint8"},{"indexed":false,"internalType":"uint256","name":"votingPower","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"VoteCast","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"delegator","type":"address"},{"indexed":true,"internalType":"address","name":"delegatee","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"timestamp","type":"uint256"}],"name":"VotingPowerDelegated","type":"event"}]`

var contractEvents = []string{
	"EmergencyPaused(address,uint256)",
	"EmergencyUnpaused(address,uint256)",
	"FundsDeposited(address,uint256,uint256)",
	"FundsWithdrawn(address,uint256,uint256,uint256)",
	"MemberAdded(address,uint8,uint256,uint256)",
	"MemberRemoved(address,uint256)",
	"MemberRoleChanged(address,uint8,uint8,uint256)",
	"ProposalCancelled(uint256,address,uint256)",
	"ProposalCreated(uint256,address,string,uint256,uint256)",
	"ProposalExecuted(uint256,address,uint256)",
	"QuorumReached(uint256,uint256,uint256)",
	"VoteCast(uint256,address,uint8,uint256,uint256)",
	"VotingPowerDelegated(address,address,uint256,uint256)",
}

func main() {
	cfg := config.MongoDBConfig()
	store, err := storage.NewMongoDB(&cfg)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer store.Close(context.Background())

	if err := store.EnsureIndexes(context.Background()); err != nil {
		log.Fatalf("failed to ensure indexes: %v", err)
	}

	ctx := context.Background()
	now := time.Now()

	// Upsert network
	network := models.Network{
		Name:      "Sepolia",
		ChainID:   sepoliaChainID,
		Type:      models.NetworkTestnet,
		RPCURLs:   sepoliaRPCURLs,
		Status:    models.StatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	filter := bson.M{"chain_id": sepoliaChainID}
	update := bson.M{
		"$set": bson.M{
			"name":       network.Name,
			"type":       network.Type,
			"rpc_urls":   network.RPCURLs,
			"status":     network.Status,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err = store.Networks().UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Fatalf("failed to upsert network: %v", err)
	}
	log.Println("upserted network: Sepolia (chain_id: 11155111)")

	var net models.Network
	if err := store.Networks().FindOne(ctx, filter).Decode(&net); err != nil {
		log.Fatalf("failed to find network: %v", err)
	}

	// Minify ABI
	abiMinified, err := utils.MinifyABI([]byte(contractABIRaw))
	if err != nil {
		log.Fatalf("failed to minify ABI: %v", err)
	}

	// Upsert contract
	addr := strings.ToLower(contractAddress)
	contractFilter := bson.M{"network_id": net.ID, "address": addr}
	contractUpdate := bson.M{
		"$set": bson.M{
			"abi":                string(abiMinified),
			"events":             contractEvents,
			"start_block":        contractStartBlock,
			"last_scanned_block": 0,
			"status":             models.StatusActive,
			"updated_at":         now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}
	contractOpts := options.Update().SetUpsert(true)

	cResult, err := store.Contracts().UpdateOne(ctx, contractFilter, contractUpdate, contractOpts)
	if err != nil {
		log.Fatalf("failed to upsert contract: %v", err)
	}

	if cResult.UpsertedCount > 0 {
		log.Printf("created contract: %s (start_block: %d)", addr, contractStartBlock)
	} else if cResult.ModifiedCount > 0 {
		log.Printf("updated contract: %s", addr)
	} else {
		log.Printf("contract unchanged: %s", addr)
	}

	log.Println("seed completed successfully")
}
