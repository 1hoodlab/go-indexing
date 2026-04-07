package solana

import (
	"context"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// These tests require a running MongoDB. Run with:
// MONGODB_TEST_URI=mongodb://localhost:27017 go test ./internal/chains/solana/... -run TestStorage -v
func setupTestStorage(t *testing.T) (*Storage, func()) {
	t.Helper()
	uri := "mongodb://localhost:27017"
	dbName := "indexing_solana_test_" + primitive.NewObjectID().Hex()

	s, err := NewStorage(uri, dbName)
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	cleanup := func() {
		_ = s.database.Drop(context.Background())
		_ = s.Close(context.Background())
	}
	return s, cleanup
}

func TestStorage_SaveEvents(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	networkID := primitive.NewObjectID()
	programID := primitive.NewObjectID()

	events := []core.RawEvent{
		{
			BlockNumber:     500,
			TransactionHash: "sig1",
			Address:         "TokenkegQ",
			EventKey:        "TokenkegQ:aabbccdd00112233",
			RawData:         []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x11, 0x22, 0x33, 0xFF},
			Metadata: map[string]string{
				"network_id":   networkID.Hex(),
				"program_id":   programID.Hex(),
				"block_hash":   "bhash1",
				"slot":         "500",
				"log_index":    "2",
				"account_keys": `["TokenkegQ","user1"]`,
			},
		},
	}

	if err := s.SaveEvents(context.Background(), events); err != nil {
		t.Fatalf("SaveEvents failed: %v", err)
	}

	var doc ProgramLog
	err := s.ProgramLogs().FindOne(context.Background(), bson.M{"transaction_hash": "sig1"}).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if doc.Slot != 500 {
		t.Errorf("expected slot 500, got %d", doc.Slot)
	}
	if doc.Discriminator != "aabbccdd00112233" {
		t.Errorf("expected discriminator aabbccdd00112233, got %s", doc.Discriminator)
	}
}

func TestStorage_SaveEvents_Idempotent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	networkID := primitive.NewObjectID()
	programID := primitive.NewObjectID()
	event := core.RawEvent{
		BlockNumber:     600,
		TransactionHash: "sig2",
		Address:         "Prog1",
		EventKey:        "Prog1:0102030405060708",
		RawData:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		Metadata: map[string]string{
			"network_id":   networkID.Hex(),
			"program_id":   programID.Hex(),
			"slot":         "600",
			"log_index":    "0",
			"block_hash":   "bh",
			"account_keys": "[]",
		},
	}

	// Insert twice; second should be ignored (unique index).
	if err := s.SaveEvents(context.Background(), []core.RawEvent{event}); err != nil {
		t.Fatalf("first SaveEvents failed: %v", err)
	}
	if err := s.SaveEvents(context.Background(), []core.RawEvent{event}); err != nil {
		t.Fatalf("second SaveEvents should be idempotent, got: %v", err)
	}

	count, _ := s.ProgramLogs().CountDocuments(context.Background(), bson.M{"transaction_hash": "sig2"})
	if count != 1 {
		t.Errorf("expected 1 document after idempotent insert, got %d", count)
	}
}

func TestStorage_UpdateScanProgress(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	networkID := primitive.NewObjectID()
	prog := Program{
		NetworkID:  networkID,
		Address:    "ProgA",
		Status:     models.StatusActive,
		StartSlot:  100,
		Events:     []string{"Transfer"},
	}
	_, err := s.Programs().InsertOne(context.Background(), prog)
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	if err := s.UpdateScanProgress(context.Background(), "ProgA", 999); err != nil {
		t.Fatalf("UpdateScanProgress: %v", err)
	}

	var updated Program
	_ = s.Programs().FindOne(context.Background(), bson.M{"address": "ProgA"}).Decode(&updated)
	if updated.LastScannedSlot != 999 {
		t.Errorf("expected last_scanned_slot=999, got %d", updated.LastScannedSlot)
	}
}

func TestStorage_LoadContracts(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	networkID := primitive.NewObjectID()
	progs := []interface{}{
		Program{
			NetworkID:       networkID,
			Address:         "ProgA",
			Events:          []string{"Transfer"},
			StartSlot:       100,
			LastScannedSlot: 200,
			Status:          models.StatusActive,
		},
		Program{
			NetworkID: networkID,
			Address:   "ProgB",
			Events:    []string{"Mint"},
			StartSlot: 50,
			Status:    models.StatusActive,
		},
		Program{NetworkID: networkID, Address: "ProgC", Status: "deactive"},
	}
	_, _ = s.Programs().InsertMany(context.Background(), progs)

	contracts, err := s.LoadContracts(context.Background())
	if err != nil {
		t.Fatalf("LoadContracts: %v", err)
	}
	if len(contracts) != 2 {
		t.Fatalf("expected 2 active programs, got %d", len(contracts))
	}
}
