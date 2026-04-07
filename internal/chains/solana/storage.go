package solana

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

// Storage implements core.StorageInterface for Solana using MongoDB.
type Storage struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewStorage connects to MongoDB and returns a Storage.
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

func (s *Storage) Networks() *mongo.Collection    { return s.database.Collection("networks") }
func (s *Storage) Programs() *mongo.Collection    { return s.database.Collection("programs") }
func (s *Storage) ProgramLogs() *mongo.Collection { return s.database.Collection("program_logs") }
func (s *Storage) DecodedEvents() *mongo.Collection {
	return s.database.Collection("decoded_events")
}

// SaveEvents inserts raw program log events into the program_logs collection.
func (s *Storage) SaveEvents(ctx context.Context, events []core.RawEvent) error {
	if len(events) == 0 {
		return nil
	}
	now := time.Now()
	docs := make([]interface{}, 0, len(events))
	for _, ev := range events {
		networkID, _ := primitive.ObjectIDFromHex(ev.Metadata["network_id"])
		programID, _ := primitive.ObjectIDFromHex(ev.Metadata["program_id"])
		logIndex, _ := strconv.Atoi(ev.Metadata["log_index"])

		var accountKeys []string
		_ = json.Unmarshal([]byte(ev.Metadata["account_keys"]), &accountKeys)

		discriminator := ""
		if len(ev.RawData) >= 8 {
			discriminator = hex.EncodeToString(ev.RawData[:8])
		}

		doc := &ProgramLog{
			NetworkID:       networkID,
			ProgramID:       programID,
			Slot:            ev.BlockNumber,
			BlockHash:       ev.Metadata["block_hash"],
			TransactionHash: ev.TransactionHash,
			LogIndex:        logIndex,
			ProgramAddress:  ev.Address,
			Discriminator:   discriminator,
			EventKey:        ev.EventKey,
			RawData:         base64.StdEncoding.EncodeToString(ev.RawData),
			AccountKeys:     accountKeys,
			CreatedAt:       now,
		}
		docs = append(docs, doc)
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err := s.ProgramLogs().InsertMany(ctx, docs, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			log.Printf("solana storage: skipped duplicate program logs")
			return nil
		}
		return err
	}
	return nil
}

// SaveDecodedEvents inserts decoded events into the decoded_events collection.
func (s *Storage) SaveDecodedEvents(ctx context.Context, events []interface{}) error {
	if len(events) == 0 {
		return nil
	}
	opts := options.InsertMany().SetOrdered(false)
	_, err := s.DecodedEvents().InsertMany(ctx, events, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			log.Printf("solana storage: skipped duplicate decoded events")
			return nil
		}
		return err
	}
	return nil
}

// UpdateScanProgress updates last_scanned_slot for the given program address.
func (s *Storage) UpdateScanProgress(ctx context.Context, address string, slot uint64) error {
	_, err := s.Programs().UpdateOne(ctx,
		bson.M{"address": address, "status": models.StatusActive},
		bson.M{"$set": bson.M{"last_scanned_slot": slot, "updated_at": time.Now()}},
	)
	return err
}

// LoadContracts loads all active programs and maps them to core.ContractConfig.
func (s *Storage) LoadContracts(ctx context.Context) ([]core.ContractConfig, error) {
	cursor, err := s.Programs().Find(ctx, bson.M{"status": models.StatusActive})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var programs []Program
	if err := cursor.All(ctx, &programs); err != nil {
		return nil, err
	}

	configs := make([]core.ContractConfig, len(programs))
	for i, prog := range programs {
		configs[i] = core.ContractConfig{
			Address:     prog.Address,
			Events:      prog.Events,
			StartBlock:  prog.StartSlot,
			LastScanned: prog.LastScannedSlot,
		}
	}
	return configs, nil
}

// EnsureIndexes creates all required MongoDB indexes.
func (s *Storage) EnsureIndexes(ctx context.Context) error {
	// Networks.
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := s.Networks().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	// Programs.
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "address", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := s.Programs().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	// ProgramLogs.
	for _, model := range []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "network_id", Value: 1},
				{Key: "program_id", Value: 1},
				{Key: "slot", Value: 1},
				{Key: "transaction_hash", Value: 1},
				{Key: "log_index", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{Keys: bson.D{{Key: "program_address", Value: 1}, {Key: "discriminator", Value: 1}}},
		{Keys: bson.D{{Key: "slot", Value: 1}}},
		{Keys: bson.D{{Key: "transaction_hash", Value: 1}}},
	} {
		if _, err := s.ProgramLogs().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	// DecodedEvents.
	for _, model := range []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "program_address", Value: 1},
				{Key: "event_name", Value: 1},
				{Key: "slot", Value: 1},
				{Key: "transaction_hash", Value: 1},
				{Key: "log_index", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{Keys: bson.D{{Key: "event_name", Value: 1}}},
		{Keys: bson.D{{Key: "slot", Value: 1}}},
	} {
		if _, err := s.DecodedEvents().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}

// LoadNetwork loads the first active network from the networks collection.
func (s *Storage) LoadNetwork(ctx context.Context) (*models.Network, error) {
	var network models.Network
	err := s.Networks().FindOne(ctx, bson.M{"status": models.StatusActive}).Decode(&network)
	if err != nil {
		return nil, err
	}
	return &network, nil
}

// LoadProgramModels loads full Program documents for active programs.
func (s *Storage) LoadProgramModels(ctx context.Context) ([]*Program, error) {
	cursor, err := s.Programs().Find(ctx, bson.M{"status": models.StatusActive})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var programs []*Program
	if err := cursor.All(ctx, &programs); err != nil {
		return nil, err
	}
	return programs, nil
}

func (s *Storage) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }
