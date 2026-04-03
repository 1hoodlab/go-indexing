package evm

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Storage struct {
	client   *mongo.Client
	database *mongo.Database
}

func NewStorage(mongoURI, database string) (*Storage, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}
	return &Storage{client: client, database: client.Database(database)}, nil
}

func (s *Storage) Database() *mongo.Database   { return s.database }
func (s *Storage) Networks() *mongo.Collection  { return s.database.Collection("networks") }
func (s *Storage) Contracts() *mongo.Collection { return s.database.Collection("contracts") }
func (s *Storage) Logs() *mongo.Collection      { return s.database.Collection("logs") }

func (s *Storage) SaveEvents(ctx context.Context, events []core.RawEvent) error {
	if len(events) == 0 {
		return nil
	}
	now := time.Now()
	docs := make([]interface{}, 0, len(events))
	for _, ev := range events {
		networkID, _ := primitive.ObjectIDFromHex(ev.Metadata["network_id"])
		contractID, _ := primitive.ObjectIDFromHex(ev.Metadata["contract_id"])
		logIndex, _ := strconv.ParseUint(ev.Metadata["log_index"], 10, 32)
		topics := []string{}
		if t := ev.Metadata["topics"]; t != "" {
			topics = strings.Split(t, ",")
		}
		doc := &Log{
			NetworkID:       networkID,
			ContractID:      contractID,
			BlockNumber:     ev.BlockNumber,
			BlockHash:       ev.Metadata["block_hash"],
			TransactionHash: ev.TransactionHash,
			LogIndex:        uint32(logIndex),
			Address:         ev.Address,
			EventSignature:  ev.EventKey,
			Topics:          topics,
			Data:            ev.Metadata["data"],
			CreatedAt:       now,
		}
		docs = append(docs, doc)
	}
	opts := options.InsertMany().SetOrdered(false)
	_, err := s.Logs().InsertMany(ctx, docs, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			log.Printf("evm storage: skipped duplicate logs (block reprocess)")
			return nil
		}
		return err
	}
	return nil
}

func (s *Storage) UpdateScanProgress(ctx context.Context, address string, block uint64) error {
	_, err := s.Contracts().UpdateOne(ctx,
		bson.M{"address": strings.ToLower(address), "status": models.StatusActive},
		bson.M{"$set": bson.M{"last_scanned_block": block, "updated_at": time.Now()}},
	)
	return err
}

func (s *Storage) LoadContracts(ctx context.Context) ([]core.ContractConfig, error) {
	cursor, err := s.Contracts().Find(ctx, bson.M{"status": models.StatusActive})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var contracts []Contract
	if err := cursor.All(ctx, &contracts); err != nil {
		return nil, err
	}
	configs := make([]core.ContractConfig, len(contracts))
	for i, c := range contracts {
		configs[i] = core.ContractConfig{
			Address:     c.Address,
			Events:      c.Events,
			StartBlock:  c.StartBlock,
			LastScanned: c.LastScannedBlock,
		}
	}
	return configs, nil
}

func (s *Storage) LoadContractModels(ctx context.Context) ([]*Contract, error) {
	cursor, err := s.Contracts().Find(ctx, bson.M{"status": models.StatusActive})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var contracts []*Contract
	if err := cursor.All(ctx, &contracts); err != nil {
		return nil, err
	}
	return contracts, nil
}

func (s *Storage) EnsureIndexes(ctx context.Context) error {
	// Network indexes
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "chain_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := s.Networks().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	// Contract indexes
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "address", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
	} {
		if _, err := s.Contracts().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	// Log indexes
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "contract_id", Value: 1}, {Key: "block_number", Value: 1}, {Key: "log_index", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
		{Keys: bson.D{{Key: "contract_id", Value: 1}}},
		{Keys: bson.D{{Key: "block_number", Value: 1}}},
		{Keys: bson.D{{Key: "transaction_hash", Value: 1}}},
		{Keys: bson.D{{Key: "address", Value: 1}, {Key: "event_signature", Value: 1}}},
	} {
		if _, err := s.Logs().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}

func (s *Storage) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
