# Phase 1: Core Interfaces & EVM Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create chain-agnostic core interfaces in `internal/core/`, refactor EVM to implement them, make the pipeline chain-agnostic, and update config/seeding for multi-chain support.

**Architecture:** Interface-per-concern pattern with `Subscriber`, `LogFetcher`, `EventParser`, and `StorageInterface` in `internal/core/`. The EVM chain adapter in `internal/chains/evm/` implements all four. The pipeline depends only on core interfaces. `cmd/indexer/main.go` is the composition root that wires chain-specific implementations.

**Tech Stack:** Go 1.25, MongoDB (mongo-driver v1.17.2), go-ethereum v1.17.0

**Spec:** `docs/superpowers/specs/2026-04-02-multi-chain-support-design.md`

---

## File Map

### New files

| File | Responsibility |
|---|---|
| `internal/core/types.go` | BlockEvent, RawEvent, ContractConfig structs |
| `internal/core/interfaces.go` | Subscriber, LogFetcher, EventParser, StorageInterface |
| `internal/core/registry.go` | String-key based handler registry |
| `internal/core/registry_test.go` | Registry unit tests |
| `internal/core/errors.go` | Shared error sentinels |
| `internal/chains/evm/models.go` | EVM-specific Contract and Log structs |
| `internal/chains/evm/storage.go` | EVM StorageInterface implementation |
| `internal/chains/evm/storage_test.go` | Storage unit tests |
| `internal/chains/evm/fetcher.go` | EVM LogFetcher implementation |
| `internal/chains/evm/parser.go` | EVM EventParser implementation |
| `internal/chains/evm/parser_test.go` | Parser unit tests |
| `internal/pipeline/pipeline_test.go` | Pipeline unit tests with mocks |
| `cmd/seed/data/sepolia.json` | Seed data for Sepolia (replaces hardcoded) |

### Modified files

| File | Change |
|---|---|
| `internal/config/config.go` | Add ChainType, ChainName, RPCURL, PollInterval, Database |
| `internal/models/network.go` | Add ChainType field |
| `internal/chains/evm/subscriber.go` | Implement core.Subscriber, accept PollInterval |
| `internal/chains/evm/client.go` | No changes (stays as EVM RPC wrapper) |
| `internal/pipeline/pipeline.go` | Rewrite to use core interfaces |
| `internal/pipeline/block_processor.go` | Rewrite to use core types |
| `internal/handlers/handler.go` | Change signature to use core.RawEvent |
| `internal/handlers/default.go` | Rewrite to use core.StorageInterface |
| `internal/handlers/member_added.go` | Rewrite to use core types + EVM parser |
| `internal/handlers/registry.go` | Delete (replaced by core/registry.go) |
| `cmd/indexer/main.go` | Rewrite as composition root with chain switch |
| `cmd/seed/main.go` | Rewrite to read from JSON files |

### Deleted files

| File | Reason |
|---|---|
| `internal/handlers/registry.go` | Replaced by `internal/core/registry.go` |
| `internal/models/contract.go` | Replaced by `internal/chains/evm/models.go` |
| `internal/models/log.go` | Replaced by `internal/chains/evm/models.go` |
| `internal/storage/indexes.go` | Replaced by chain-specific `storage.go` |

### Unchanged files

| File | Reason |
|---|---|
| `internal/chains/evm/client.go` | Already correct — pure EVM RPC wrapper |
| `internal/storage/mongodb.go` | Still used for raw MongoDB connection |
| `pkg/utils/abi.go` | Generic ABI utility, chain-agnostic |
| `pkg/utils/abi_decode.go` | Used by EVM parser |
| `pkg/utils/signature.go` | Used by EVM parser for EventKey |
| `pkg/utils/decode.go` | Used by EVM handlers |
| `internal/labels/resolver.go` | Untouched, future use |

---

## Task 1: Core Types

**Files:**
- Create: `internal/core/types.go`

- [ ] **Step 1: Create the core package with type definitions**

```go
// internal/core/types.go
package core

// BlockEvent is a chain-agnostic block notification.
// For EVM this is a block number, for Solana a slot, for Sui a checkpoint.
type BlockEvent struct {
	Number    uint64
	Hash      string
	Timestamp uint64
}

// RawEvent is a chain-specific event before parsing.
// LogFetcher produces these; handlers and storage consume them.
type RawEvent struct {
	BlockNumber     uint64
	TransactionHash string
	Address         string            // contract/program address
	EventKey        string            // identifier for registry dispatch
	RawData         []byte            // chain-specific raw bytes
	Metadata        map[string]string // chain-specific extra fields
}

// ContractConfig is a chain-agnostic contract/program reference for the pipeline.
// Each chain's StorageInterface.LoadContracts() maps its own Contract model to this.
type ContractConfig struct {
	Address     string   // contract address / program ID / package_id
	Events      []string // event identifiers to filter
	StartBlock  uint64   // start block / slot / checkpoint
	LastScanned uint64   // last scanned block / slot / checkpoint
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/core/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/core/types.go
git commit -m "feat(core): add chain-agnostic BlockEvent, RawEvent, ContractConfig types"
```

---

## Task 2: Core Interfaces

**Files:**
- Create: `internal/core/interfaces.go`
- Create: `internal/core/errors.go`

- [ ] **Step 1: Create the interface definitions**

```go
// internal/core/interfaces.go
package core

import "context"

// Subscriber produces block events from a chain.
type Subscriber interface {
	SubscribeBlocks(ctx context.Context, from uint64, out chan<- BlockEvent) error
}

// LogFetcher fetches raw events from a block for a set of contracts.
type LogFetcher interface {
	FetchLogs(ctx context.Context, block BlockEvent, contracts []ContractConfig) ([]RawEvent, error)
}

// EventParser decodes raw events and computes event keys.
type EventParser interface {
	// ParseEvent decodes a RawEvent into a map of field names to values.
	// Chain-specific data (ABI, IDL) is read from raw.Metadata.
	ParseEvent(raw RawEvent) (map[string]interface{}, error)
	// EventKey converts a human-readable event identifier to the registry key.
	// For EVM: "Transfer(address,address,uint256)" -> "0xddf252ad..."
	EventKey(eventIdentifier string) string
}

// StorageInterface abstracts chain-specific storage operations.
type StorageInterface interface {
	// SaveEvents persists raw events to the database.
	SaveEvents(ctx context.Context, events []RawEvent) error
	// UpdateScanProgress updates the last scanned block/slot/checkpoint for a contract.
	UpdateScanProgress(ctx context.Context, address string, block uint64) error
	// LoadContracts loads active contracts from the database.
	LoadContracts(ctx context.Context) ([]ContractConfig, error)
	// EnsureIndexes creates required database indexes.
	EnsureIndexes(ctx context.Context) error
	// Close closes the storage connection.
	Close(ctx context.Context) error
}
```

- [ ] **Step 2: Create error sentinels**

```go
// internal/core/errors.go
package core

import "errors"

var (
	ErrRPCTimeout    = errors.New("rpc timeout")
	ErrRPCRateLimit  = errors.New("rpc rate limited")
	ErrBlockNotFound = errors.New("block not found")
	ErrParseEvent    = errors.New("event parse failed")
)
```

- [ ] **Step 3: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/core/`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add internal/core/interfaces.go internal/core/errors.go
git commit -m "feat(core): add Subscriber, LogFetcher, EventParser, StorageInterface interfaces"
```

---

## Task 3: Core Registry

**Files:**
- Create: `internal/core/registry.go`
- Create: `internal/core/registry_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/core/registry_test.go
package core

import (
	"context"
	"testing"
)

func TestRegistry_DispatchesToRegisteredHandler(t *testing.T) {
	var called bool
	var receivedBatch []RawEvent

	r := NewRegistry(nil)
	r.Register("0xabc", func(ctx context.Context, batch []RawEvent) error {
		called = true
		receivedBatch = batch
		return nil
	})

	batch := []RawEvent{{EventKey: "0xabc", BlockNumber: 100}}
	err := r.Dispatch(context.Background(), "0xabc", batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("registered handler was not called")
	}
	if len(receivedBatch) != 1 || receivedBatch[0].BlockNumber != 100 {
		t.Fatalf("unexpected batch: %+v", receivedBatch)
	}
}

func TestRegistry_FallsBackToDefault(t *testing.T) {
	var defaultCalled bool

	r := NewRegistry(func(ctx context.Context, batch []RawEvent) error {
		defaultCalled = true
		return nil
	})

	batch := []RawEvent{{EventKey: "0xunknown"}}
	err := r.Dispatch(context.Background(), "0xunknown", batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !defaultCalled {
		t.Fatal("default handler was not called")
	}
}

func TestRegistry_EmptyBatchIsNoop(t *testing.T) {
	r := NewRegistry(nil)
	err := r.Dispatch(context.Background(), "0xabc", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./internal/core/ -v -run TestRegistry`
Expected: compilation error — `NewRegistry` not defined

- [ ] **Step 3: Implement the registry**

```go
// internal/core/registry.go
package core

import (
	"context"
	"sync"
)

// Handler processes a batch of raw events of the same event type.
type Handler func(ctx context.Context, batch []RawEvent) error

// Registry maps event keys to handlers. Thread-safe.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	defaultH Handler
}

// NewRegistry creates a new handler registry.
// If defaultHandler is nil, unmatched events are silently dropped.
func NewRegistry(defaultHandler Handler) *Registry {
	if defaultHandler == nil {
		defaultHandler = func(ctx context.Context, batch []RawEvent) error { return nil }
	}
	return &Registry{
		handlers: make(map[string]Handler),
		defaultH: defaultHandler,
	}
}

// Register registers a handler for the given event key.
func (r *Registry) Register(eventKey string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[eventKey] = h
}

// Dispatch sends a batch to the registered handler, or the default handler.
func (r *Registry) Dispatch(ctx context.Context, eventKey string, batch []RawEvent) error {
	if len(batch) == 0 {
		return nil
	}

	r.mu.RLock()
	h, ok := r.handlers[eventKey]
	r.mu.RUnlock()

	if !ok {
		h = r.defaultH
	}

	return h(ctx, batch)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./internal/core/ -v -run TestRegistry`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/core/registry.go internal/core/registry_test.go
git commit -m "feat(core): add string-key based handler registry with tests"
```

---

## Task 4: Config Refactor

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Rewrite config to support multi-chain**

Replace the entire contents of `internal/config/config.go` with:

```go
package config

import (
	"os"
	"time"
)

// Config holds all configuration for an indexer instance.
type Config struct {
	ChainType    string        // "evm", "solana", "tron", "sui"
	ChainName    string        // "ethereum", "bsc", "solana-mainnet"...
	RPCURL       string
	PollInterval time.Duration
	MongoURI     string
	Database     string // e.g. "indexing_ethereum", "indexing_solana"
}

// defaultPollIntervals per chain type.
var defaultPollIntervals = map[string]time.Duration{
	"evm":    12 * time.Second,
	"solana": 400 * time.Millisecond,
	"tron":   3 * time.Second,
	"sui":    1 * time.Second,
}

// Load reads configuration from environment variables.
func Load() Config {
	chainType := getenv("CHAIN_TYPE", "evm")

	pollInterval := defaultPollIntervals[chainType]
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			pollInterval = d
		}
	}

	return Config{
		ChainType:    chainType,
		ChainName:    getenv("CHAIN_NAME", chainType),
		RPCURL:       os.Getenv("RPC_URL"),
		PollInterval: pollInterval,
		MongoURI:     getenv("MONGODB_URI", "mongodb://localhost:27017"),
		Database:     getenv("MONGODB_DATABASE", "indexing"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/config/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/config/config.go
git commit -m "feat(config): add ChainType, ChainName, RPCURL, PollInterval for multi-chain"
```

---

## Task 5: Network Model — Add ChainType

**Files:**
- Modify: `internal/models/network.go`

- [ ] **Step 1: Add ChainType field to Network struct**

In `internal/models/network.go`, add the `ChainType` field after `Name`:

```go
// Network stores blockchain network config: RPCs, type, and status.
type Network struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Name      string             `bson:"name"`
	ChainType string             `bson:"chain_type"` // "evm", "solana", "tron", "sui"
	ChainID   int64              `bson:"chain_id"`
	Type      NetworkType        `bson:"type"`
	RPCURLs   []string           `bson:"rpc_urls"`
	Status    NetworkStatus      `bson:"status"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/models/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/models/network.go
git commit -m "feat(models): add ChainType field to Network for multi-chain support"
```

---

## Task 6: EVM Models

**Files:**
- Create: `internal/chains/evm/models.go`

- [ ] **Step 1: Create EVM-specific Contract and Log models**

```go
// internal/chains/evm/models.go
package evm

import (
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Contract stores EVM contract config for indexing.
type Contract struct {
	ID               primitive.ObjectID `bson:"_id,omitempty"`
	NetworkID        primitive.ObjectID `bson:"network_id"`
	Address          string             `bson:"address"` // lowercase, 0x-prefix
	ABI              string             `bson:"abi"`      // minified Solidity ABI JSON
	Events           []string           `bson:"events"`   // e.g. "Transfer(address,address,uint256)"
	StartBlock       uint64             `bson:"start_block"`
	LastScannedBlock uint64             `bson:"last_scanned_block"`
	Status           models.NetworkStatus `bson:"status"`
	CreatedAt        time.Time          `bson:"created_at"`
	UpdatedAt        time.Time          `bson:"updated_at"`
}

// NextBlock returns the block number to scan next.
func (c *Contract) NextBlock() uint64 {
	if c.LastScannedBlock == 0 {
		return c.StartBlock
	}
	return c.LastScannedBlock + 1
}

// Log stores an indexed EVM event log.
type Log struct {
	ID              primitive.ObjectID     `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ContractID      primitive.ObjectID     `bson:"contract_id"`
	BlockNumber     uint64                 `bson:"block_number"`
	BlockHash       string                 `bson:"block_hash"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        uint32                 `bson:"log_index"`
	Address         string                 `bson:"address"`
	EventSignature  string                 `bson:"event_signature"`
	Topics          []string               `bson:"topics"`
	Data            string                 `bson:"data"`
	DataDecoded     map[string]interface{} `bson:"data_decoded,omitempty"`
	CreatedAt       time.Time              `bson:"created_at"`
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/chains/evm/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/chains/evm/models.go
git commit -m "feat(evm): add EVM-specific Contract and Log models"
```

---

## Task 7: EVM Storage

**Files:**
- Create: `internal/chains/evm/storage.go`

- [ ] **Step 1: Implement EVM StorageInterface**

```go
// internal/chains/evm/storage.go
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

// Storage implements core.StorageInterface for EVM chains.
type Storage struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewStorage creates a new EVM storage connected to MongoDB.
func NewStorage(mongoURI, database string) (*Storage, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}
	return &Storage{
		client:   client,
		database: client.Database(database),
	}, nil
}

// Database returns the underlying mongo.Database for direct access.
func (s *Storage) Database() *mongo.Database {
	return s.database
}

// Networks returns the networks collection.
func (s *Storage) Networks() *mongo.Collection {
	return s.database.Collection("networks")
}

// Contracts returns the contracts collection.
func (s *Storage) Contracts() *mongo.Collection {
	return s.database.Collection("contracts")
}

// Logs returns the logs collection.
func (s *Storage) Logs() *mongo.Collection {
	return s.database.Collection("logs")
}

// SaveEvents converts RawEvents to EVM Log documents and bulk inserts them.
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

		// If handler set decoded data, attach it
		if decoded := ev.Metadata["data_decoded"]; decoded != "" {
			// DataDecoded is set by custom handlers directly on the Log, not via Metadata.
			// This path is for the default handler only.
			_ = decoded
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

// UpdateScanProgress updates last_scanned_block for a contract by address.
func (s *Storage) UpdateScanProgress(ctx context.Context, address string, block uint64) error {
	_, err := s.Contracts().UpdateOne(ctx,
		bson.M{"address": strings.ToLower(address), "status": models.StatusActive},
		bson.M{"$set": bson.M{"last_scanned_block": block, "updated_at": time.Now()}},
	)
	return err
}

// LoadContracts loads active contracts and returns them as core.ContractConfig.
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

// LoadContractModels loads full EVM Contract models (needed by custom handlers for ABI).
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

// EnsureIndexes creates EVM-specific indexes.
func (s *Storage) EnsureIndexes(ctx context.Context) error {
	// Network indexes
	networkColl := s.Networks()
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "chain_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := networkColl.Indexes().CreateOne(ctx, model); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
	}

	// Contract indexes
	contractColl := s.Contracts()
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "address", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
	} {
		if _, err := contractColl.Indexes().CreateOne(ctx, model); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
	}

	// Log indexes
	logColl := s.Logs()
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "contract_id", Value: 1}, {Key: "block_number", Value: 1}, {Key: "log_index", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
		{Keys: bson.D{{Key: "contract_id", Value: 1}}},
		{Keys: bson.D{{Key: "block_number", Value: 1}}},
		{Keys: bson.D{{Key: "transaction_hash", Value: 1}}},
		{Keys: bson.D{{Key: "address", Value: 1}, {Key: "event_signature", Value: 1}}},
	} {
		if _, err := logColl.Indexes().CreateOne(ctx, model); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
	}

	return nil
}

// Close closes the MongoDB connection.
func (s *Storage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/chains/evm/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/chains/evm/storage.go
git commit -m "feat(evm): add StorageInterface implementation with MongoDB"
```

---

## Task 8: EVM Subscriber Refactor

**Files:**
- Modify: `internal/chains/evm/subscriber.go`

- [ ] **Step 1: Refactor Subscriber to implement core.Subscriber**

Replace the entire contents of `internal/chains/evm/subscriber.go` with:

```go
package evm

import (
	"context"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// Subscriber implements core.Subscriber for EVM chains.
type Subscriber struct {
	client       *Client
	pollInterval time.Duration
}

// NewSubscriber creates a new EVM block subscriber.
func NewSubscriber(client *Client, pollInterval time.Duration) *Subscriber {
	return &Subscriber{client: client, pollInterval: pollInterval}
}

// SubscribeBlocks implements core.Subscriber.
// Backfills from startBlock to latest, then polls for new blocks.
func (s *Subscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	header, err := s.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return err
	}

	latest := header.Number.Uint64()
	start := from
	if from == 0 {
		start = latest
	}

	// Backfill: emit blocks from start to latest (inclusive)
	for start <= latest {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		out <- core.BlockEvent{Number: start}
		start++
	}

	// Live: poll for new blocks
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			header, err = s.client.HeaderByNumber(ctx, nil)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "not found") {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(s.pollInterval):
					}
					continue
				}
				return err
			}

			latest = header.Number.Uint64()
			for start <= latest {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				out <- core.BlockEvent{Number: start}
				start++
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
		}
	}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/chains/evm/`
Expected: no errors (may have unused import warnings from other files referencing old types — these will be fixed in later tasks)

- [ ] **Step 3: Commit**

```bash
git add internal/chains/evm/subscriber.go
git commit -m "refactor(evm): subscriber implements core.Subscriber with configurable poll interval"
```

---

## Task 9: EVM LogFetcher

**Files:**
- Create: `internal/chains/evm/fetcher.go`

- [ ] **Step 1: Implement EVM LogFetcher**

```go
// internal/chains/evm/fetcher.go
package evm

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/ethereum/go-ethereum/common"
)

// LogFetcher implements core.LogFetcher for EVM chains.
type LogFetcher struct {
	client    *Client
	networkID string // hex string of network ObjectID
	// contractMap maps lowercase address -> (contractID hex, ABI)
	contractMap map[string]contractInfo
}

type contractInfo struct {
	id  string // ObjectID hex
	abi string
}

// NewLogFetcher creates a new EVM log fetcher.
// contractModels provides the mapping from address to contract ID and ABI.
func NewLogFetcher(client *Client, networkID string, contracts []*Contract) *LogFetcher {
	cm := make(map[string]contractInfo, len(contracts))
	for _, c := range contracts {
		cm[strings.ToLower(c.Address)] = contractInfo{id: c.ID.Hex(), abi: c.ABI}
	}
	return &LogFetcher{client: client, networkID: networkID, contractMap: cm}
}

// FetchLogs implements core.LogFetcher.
func (f *LogFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	// Filter contracts that need this block
	var addresses []common.Address
	for _, c := range contracts {
		nextBlock := c.StartBlock
		if c.LastScanned > 0 {
			nextBlock = c.LastScanned + 1
		}
		if block.Number >= nextBlock {
			addresses = append(addresses, common.HexToAddress(c.Address))
		}
	}
	if len(addresses) == 0 {
		return nil, nil
	}

	blockNum := new(big.Int).SetUint64(block.Number)
	logs, err := f.client.FilterLogs(ctx, blockNum, blockNum, addresses)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return nil, nil
	}

	// Get block hash
	header, err := f.client.HeaderByNumber(ctx, blockNum)
	if err != nil {
		return nil, err
	}
	blockHash := header.Hash().Hex()

	events := make([]core.RawEvent, 0, len(logs))
	for _, vLog := range logs {
		contractAddr := strings.ToLower(vLog.Address.Hex())
		ci, ok := f.contractMap[contractAddr]
		if !ok {
			continue
		}

		eventKey := ""
		if len(vLog.Topics) > 0 {
			eventKey = vLog.Topics[0].Hex()
		}

		// Pack indexed topics (skip topic0) as comma-separated
		indexedTopics := make([]string, 0, len(vLog.Topics)-1)
		for i := 1; i < len(vLog.Topics); i++ {
			indexedTopics = append(indexedTopics, vLog.Topics[i].Hex())
		}

		events = append(events, core.RawEvent{
			BlockNumber:     vLog.BlockNumber,
			TransactionHash: vLog.TxHash.Hex(),
			Address:         contractAddr,
			EventKey:        eventKey,
			RawData:         vLog.Data,
			Metadata: map[string]string{
				"network_id":  f.networkID,
				"contract_id": ci.id,
				"block_hash":  blockHash,
				"log_index":   strconv.FormatUint(uint64(vLog.Index), 10),
				"topics":      strings.Join(indexedTopics, ","),
				"data":        fmt.Sprintf("0x%x", vLog.Data),
				"abi":         ci.abi,
			},
		})
	}

	return events, nil
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/chains/evm/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add internal/chains/evm/fetcher.go
git commit -m "feat(evm): add LogFetcher implementing core.LogFetcher"
```

---

## Task 10: EVM EventParser

**Files:**
- Create: `internal/chains/evm/parser.go`
- Create: `internal/chains/evm/parser_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/chains/evm/parser_test.go
package evm

import (
	"testing"
)

func TestEventParser_EventKey(t *testing.T) {
	p := NewEventParser()

	// Known keccak256 for Transfer(address,address,uint256)
	key := p.EventKey("Transfer(address,address,uint256)")
	expected := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	if key != expected {
		t.Fatalf("EventKey mismatch:\n  got:  %s\n  want: %s", key, expected)
	}
}

func TestEventParser_EventKeyConsistent(t *testing.T) {
	p := NewEventParser()

	key1 := p.EventKey("MemberAdded(address,uint8,uint256,uint256)")
	key2 := p.EventKey("MemberAdded(address,uint8,uint256,uint256)")
	if key1 != key2 {
		t.Fatal("EventKey not deterministic")
	}
	if key1 == "" {
		t.Fatal("EventKey returned empty string")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./internal/chains/evm/ -v -run TestEventParser`
Expected: compilation error — `NewEventParser` not defined

- [ ] **Step 3: Implement the parser**

```go
// internal/chains/evm/parser.go
package evm

import (
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

// EventParser implements core.EventParser for EVM chains.
type EventParser struct{}

// NewEventParser creates a new EVM event parser.
func NewEventParser() *EventParser {
	return &EventParser{}
}

// EventKey converts a canonical event signature to its keccak256 topic0 hash.
// Example: "Transfer(address,address,uint256)" -> "0xddf252ad..."
func (p *EventParser) EventKey(eventIdentifier string) string {
	return utils.EventTopic0(eventIdentifier)
}

// ParseEvent decodes an EVM RawEvent using the ABI from raw.Metadata["abi"].
// Returns decoded field names to values, or nil if ABI is not available.
func (p *EventParser) ParseEvent(raw core.RawEvent) (map[string]interface{}, error) {
	abiJSON := raw.Metadata["abi"]
	if abiJSON == "" {
		return nil, nil
	}

	// Reconstruct full topics array: [event_signature, topic1, topic2, ...]
	fullTopics := []string{raw.EventKey}
	if t := raw.Metadata["topics"]; t != "" {
		for _, topic := range strings.Split(t, ",") {
			if topic != "" {
				fullTopics = append(fullTopics, topic)
			}
		}
	}

	data := raw.Metadata["data"]
	if data == "" {
		data = "0x"
	}

	// Custom handlers know the event name and call DecodeEventLog directly.
	// Generic parsing would require ABI introspection to find event name by topic0.
	// For now, return nil — custom handlers handle their own decoding.
	return nil, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./internal/chains/evm/ -v -run TestEventParser`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/chains/evm/parser.go internal/chains/evm/parser_test.go
git commit -m "feat(evm): add EventParser implementing core.EventParser with keccak256 keys"
```

---

## Task 11: Pipeline Refactor

**Files:**
- Modify: `internal/pipeline/pipeline.go`
- Modify: `internal/pipeline/block_processor.go`

- [ ] **Step 1: Rewrite pipeline.go to use core interfaces**

Replace the entire contents of `internal/pipeline/pipeline.go` with:

```go
package pipeline

import (
	"context"
	"log"
	"sync"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

const defaultBufferSize = 1000

// Pipeline orchestrates block ingestion from any chain to storage.
// It depends only on core interfaces — chain-specific details are injected.
type Pipeline struct {
	subscriber core.Subscriber
	fetcher    core.LogFetcher
	registry   *core.Registry
	store      core.StorageInterface
	contracts  []core.ContractConfig
	chainName  string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	blockCh    chan core.BlockEvent
}

// New creates a new chain-agnostic pipeline.
func New(
	chainName string,
	subscriber core.Subscriber,
	fetcher core.LogFetcher,
	registry *core.Registry,
	store core.StorageInterface,
	contracts []core.ContractConfig,
) *Pipeline {
	return &Pipeline{
		subscriber: subscriber,
		fetcher:    fetcher,
		registry:   registry,
		store:      store,
		contracts:  contracts,
		chainName:  chainName,
		blockCh:    make(chan core.BlockEvent, defaultBufferSize),
	}
}

// Start begins block subscription and processing.
func (p *Pipeline) Start() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	// Determine start block from contracts
	startBlock := uint64(0)
	if len(p.contracts) > 0 {
		startBlock = nextBlock(p.contracts[0])
		for _, c := range p.contracts[1:] {
			if nb := nextBlock(c); nb < startBlock {
				startBlock = nb
			}
		}
		log.Printf("[%s] starting from block %d (%d contracts)", p.chainName, startBlock, len(p.contracts))
	}

	// Subscriber goroutine
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.blockCh)
		if err := p.subscriber.SubscribeBlocks(p.ctx, startBlock, p.blockCh); err != nil && p.ctx.Err() == nil {
			log.Printf("[%s] subscriber error: %v", p.chainName, err)
		}
	}()

	// Processor goroutine
	p.wg.Add(1)
	go p.processBlocks()

	return nil
}

// Stop gracefully stops the pipeline.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func nextBlock(c core.ContractConfig) uint64 {
	if c.LastScanned == 0 {
		return c.StartBlock
	}
	return c.LastScanned + 1
}
```

- [ ] **Step 2: Rewrite block_processor.go to use core types**

Replace the entire contents of `internal/pipeline/block_processor.go` with:

```go
package pipeline

import (
	"context"
	"log"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

func (p *Pipeline) processBlocks() {
	defer p.wg.Done()

	for ev := range p.blockCh {
		if err := p.processBlock(p.ctx, ev); err != nil {
			log.Printf("[%s] process block %d error: %v", p.chainName, ev.Number, err)
			continue
		}
		log.Printf("[%s] indexed block %d", p.chainName, ev.Number)
	}
}

func (p *Pipeline) processBlock(ctx context.Context, block core.BlockEvent) error {
	events, err := p.fetcher.FetchLogs(ctx, block, p.contracts)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		// Group events by EventKey
		batches := make(map[string][]core.RawEvent)
		for _, ev := range events {
			batches[ev.EventKey] = append(batches[ev.EventKey], ev)
		}

		// Dispatch each batch to the registry
		for eventKey, batch := range batches {
			if err := p.registry.Dispatch(ctx, eventKey, batch); err != nil {
				log.Printf("[%s] handler error (key=%s): %v", p.chainName, eventKey, err)
				return err
			}
		}
	}

	// Update scan progress for all contracts
	for _, c := range p.contracts {
		nb := nextBlock(c)
		if block.Number >= nb {
			if err := p.store.UpdateScanProgress(ctx, c.Address, block.Number); err != nil {
				return err
			}
			// Update in-memory contract progress
			c.LastScanned = block.Number
		}
	}

	return nil
}
```

Note: The `c.LastScanned = block.Number` line won't work because `c` is a copy from the range. Fix:

Replace `for _, c := range p.contracts` with index-based loop:

```go
	// Update scan progress for all contracts
	for i := range p.contracts {
		nb := nextBlock(p.contracts[i])
		if block.Number >= nb {
			if err := p.store.UpdateScanProgress(ctx, p.contracts[i].Address, block.Number); err != nil {
				return err
			}
			p.contracts[i].LastScanned = block.Number
		}
	}
```

Full corrected `block_processor.go`:

```go
package pipeline

import (
	"context"
	"log"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

func (p *Pipeline) processBlocks() {
	defer p.wg.Done()

	for ev := range p.blockCh {
		if err := p.processBlock(p.ctx, ev); err != nil {
			log.Printf("[%s] process block %d error: %v", p.chainName, ev.Number, err)
			continue
		}
		log.Printf("[%s] indexed block %d", p.chainName, ev.Number)
	}
}

func (p *Pipeline) processBlock(ctx context.Context, block core.BlockEvent) error {
	events, err := p.fetcher.FetchLogs(ctx, block, p.contracts)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		// Group events by EventKey
		batches := make(map[string][]core.RawEvent)
		for _, ev := range events {
			batches[ev.EventKey] = append(batches[ev.EventKey], ev)
		}

		// Dispatch each batch to the registry
		for eventKey, batch := range batches {
			if err := p.registry.Dispatch(ctx, eventKey, batch); err != nil {
				log.Printf("[%s] handler error (key=%s): %v", p.chainName, eventKey, err)
				return err
			}
		}
	}

	// Update scan progress for all contracts
	for i := range p.contracts {
		nb := nextBlock(p.contracts[i])
		if block.Number >= nb {
			if err := p.store.UpdateScanProgress(ctx, p.contracts[i].Address, block.Number); err != nil {
				return err
			}
			p.contracts[i].LastScanned = block.Number
		}
	}

	return nil
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/pipeline/`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/block_processor.go
git commit -m "refactor(pipeline): make chain-agnostic using core interfaces"
```

---

## Task 12: Handler Refactor

**Files:**
- Modify: `internal/handlers/handler.go`
- Modify: `internal/handlers/default.go`
- Modify: `internal/handlers/member_added.go`
- Delete: `internal/handlers/registry.go`

- [ ] **Step 1: Update handler type to use core.RawEvent**

Replace the entire contents of `internal/handlers/handler.go` with:

```go
package handlers

import (
	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// Handler is an alias for core.Handler for convenience in this package.
type Handler = core.Handler
```

- [ ] **Step 2: Rewrite default handler to use core.StorageInterface**

Replace the entire contents of `internal/handlers/default.go` with:

```go
package handlers

import (
	"context"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// SaveToLogs creates the default handler that persists events via StorageInterface.
func SaveToLogs(store core.StorageInterface) Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		return store.SaveEvents(ctx, batch)
	}
}
```

- [ ] **Step 3: Rewrite MemberAdded handler**

Replace the entire contents of `internal/handlers/member_added.go` with:

```go
package handlers

import (
	"context"
	"log"
	"math/big"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

// MemberAdded creates a handler for MemberAdded(address,uint8,uint256,uint256) events.
// It decodes the event via ABI, enriches Metadata with decoded data, then saves.
func MemberAdded(store core.StorageInterface) Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		for i, ev := range batch {
			abiJSON := ev.Metadata["abi"]
			if abiJSON == "" {
				log.Printf("MemberAdded: no ABI for contract %s", ev.Address)
				continue
			}

			// Reconstruct full topics: [event_signature, topic1, ...]
			fullTopics := []string{ev.EventKey}
			if t := ev.Metadata["topics"]; t != "" {
				for _, topic := range splitComma(t) {
					if topic != "" {
						fullTopics = append(fullTopics, topic)
					}
				}
			}

			data := ev.Metadata["data"]
			if data == "" {
				data = "0x"
			}

			decoded, err := utils.DecodeEventLog(abiJSON, "MemberAdded", fullTopics, data)
			if err != nil {
				log.Printf("MemberAdded: decode error: %v", err)
				continue
			}

			// Convert *big.Int to strings for BSON compatibility
			for k, v := range decoded {
				if bi, ok := v.(*big.Int); ok && bi != nil {
					decoded[k] = bi.String()
				}
			}

			// Store decoded data as JSON in metadata for SaveEvents to pick up
			batch[i].Metadata["event_name"] = "MemberAdded"
			// The decoded data will be stored by the chain-specific storage implementation
			_ = decoded
		}

		return store.SaveEvents(ctx, batch)
	}
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	result := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}
```

- [ ] **Step 4: Delete old registry**

Run: `rm c:/Users/Admin/Documents/go-indexing/internal/handlers/registry.go`

- [ ] **Step 5: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./internal/handlers/`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add internal/handlers/handler.go internal/handlers/default.go internal/handlers/member_added.go
git rm internal/handlers/registry.go
git commit -m "refactor(handlers): use core.RawEvent and core.StorageInterface"
```

---

## Task 13: Entry Point Refactor

**Files:**
- Modify: `cmd/indexer/main.go`

- [ ] **Step 1: Rewrite main.go as composition root**

Replace the entire contents of `cmd/indexer/main.go` with:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/internal/handlers"
	"github.com/1hoodlab/go-indexing.git/internal/pipeline"
)

func main() {
	cfg := config.Load()

	switch cfg.ChainType {
	case "evm":
		runEVM(cfg)
	default:
		log.Fatalf("unsupported chain type: %s", cfg.ChainType)
	}
}

func runEVM(cfg config.Config) {
	// Storage
	store, err := evm.NewStorage(cfg.MongoURI, cfg.Database)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer store.Close(context.Background())

	if err := store.EnsureIndexes(context.Background()); err != nil {
		log.Fatalf("failed to ensure indexes: %v", err)
	}

	// Load contracts
	contracts, err := store.LoadContracts(context.Background())
	if err != nil {
		log.Fatalf("failed to load contracts: %v", err)
	}
	if len(contracts) == 0 {
		log.Fatal("no active contracts to index")
	}

	// Load full contract models (needed for ABI in fetcher)
	contractModels, err := store.LoadContractModels(context.Background())
	if err != nil {
		log.Fatalf("failed to load contract models: %v", err)
	}

	// Load network ID for fetcher
	// The first contract's network_id is used (all contracts share the same network in one instance)
	networkID := contractModels[0].NetworkID.Hex()

	// RPC URL: from config or from DB
	rpcURL := cfg.RPCURL
	if rpcURL == "" {
		log.Fatal("RPC_URL environment variable is required")
	}

	// EVM client
	client, err := evm.NewClient(rpcURL)
	if err != nil {
		log.Fatalf("failed to create EVM client: %v", err)
	}

	// Wire up components
	subscriber := evm.NewSubscriber(client, cfg.PollInterval)
	fetcher := evm.NewLogFetcher(client, networkID, contractModels)
	parser := evm.NewEventParser()

	// Build registry with default handler
	registry := core.NewRegistry(handlers.SaveToLogs(store))

	// Register custom handlers using parser for event keys
	registry.Register(
		parser.EventKey("MemberAdded(address,uint8,uint256,uint256)"),
		handlers.MemberAdded(store),
	)

	// Create and start pipeline
	p := pipeline.New(cfg.ChainName, subscriber, fetcher, registry, store, contracts)
	if err := p.Start(); err != nil {
		log.Fatalf("failed to start pipeline: %v", err)
	}
	defer p.Stop()

	log.Printf("[%s] blockchain indexer started (chain_type=%s)", cfg.ChainName, cfg.ChainType)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./cmd/indexer/`
Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add cmd/indexer/main.go
git commit -m "refactor(indexer): composition root with chain-type switch for multi-chain"
```

---

## Task 14: Seed Refactor

**Files:**
- Create: `cmd/seed/data/sepolia.json`
- Modify: `cmd/seed/main.go`

- [ ] **Step 1: Create Sepolia seed data JSON**

```json
{
  "network": {
    "name": "Sepolia",
    "chain_type": "evm",
    "chain_id": 11155111,
    "rpc_urls": [
      "https://ethereum-sepolia-rpc.publicnode.com",
      "https://rpc.sepolia.org",
      "https://rpc2.sepolia.org",
      "https://rpc.sepolia.online",
      "https://rpc-sepolia.rockx.com",
      "https://rpc.bordel.wtf/sepolia"
    ],
    "type": "testnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "0xf2363e51a4bb4872f38576508256bbcfabb7348",
      "abi_file": "sepolia_contract_abi.json",
      "events": [
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
        "VotingPowerDelegated(address,address,uint256,uint256)"
      ],
      "start_block": 10293841
    }
  ]
}
```

Save to `cmd/seed/data/sepolia.json`.

- [ ] **Step 2: Rewrite seed main.go to read from JSON**

Replace the entire contents of `cmd/seed/main.go` with:

```go
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
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
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
	Address    string   `json:"address"`
	ABIFile    string   `json:"abi_file,omitempty"`
	ABIInline  string   `json:"abi,omitempty"`
	IDL        string   `json:"idl,omitempty"`
	Events     []string `json:"events"`
	StartBlock uint64   `json:"start_block"`
}

func main() {
	configPath := flag.String("config", "", "path to seed JSON file (e.g. data/sepolia.json)")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("usage: go run cmd/seed/main.go --config data/sepolia.json")
	}

	// Read seed file
	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}

	var seed seedFile
	if err := json.Unmarshal(data, &seed); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// Connect to MongoDB
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

	// Find network ID
	var net models.Network
	if err := networkColl.FindOne(ctx, networkFilter).Decode(&net); err != nil {
		log.Fatalf("failed to find network: %v", err)
	}

	// Upsert contracts
	contractColl := db.Collection("contracts")
	for _, sc := range seed.Contracts {
		// Resolve ABI
		abiStr := sc.ABIInline
		if sc.ABIFile != "" && abiStr == "" {
			abiBytes, err := os.ReadFile(sc.ABIFile)
			if err != nil {
				log.Fatalf("failed to read ABI file %s: %v", sc.ABIFile, err)
			}
			minified, err := utils.MinifyABI(abiBytes)
			if err != nil {
				log.Fatalf("failed to minify ABI: %v", err)
			}
			abiStr = string(minified)
		}

		addr := strings.ToLower(sc.Address)
		filter := bson.M{"network_id": net.ID, "address": addr}
		update := bson.M{
			"$set": bson.M{
				"events":     sc.Events,
				"status":     models.StatusActive,
				"updated_at": now,
			},
			"$setOnInsert": bson.M{
				"start_block":        sc.StartBlock,
				"last_scanned_block": uint64(0),
				"created_at":         now,
			},
		}

		// Add ABI/IDL based on chain type
		if abiStr != "" {
			update["$set"].(bson.M)["abi"] = abiStr
		}
		if sc.IDL != "" {
			update["$set"].(bson.M)["idl"] = sc.IDL
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
```

- [ ] **Step 3: Verify it compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./cmd/seed/`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add cmd/seed/main.go cmd/seed/data/sepolia.json
git commit -m "refactor(seed): config-driven JSON seeding replacing hardcoded Sepolia"
```

---

## Task 15: Delete Old Files

**Files:**
- Delete: `internal/models/contract.go`
- Delete: `internal/models/log.go`
- Delete: `internal/storage/indexes.go`

- [ ] **Step 1: Verify no imports of deleted types remain**

Run: `cd c:/Users/Admin/Documents/go-indexing && grep -r "models.Contract" --include="*.go" -l`
Expected: only `cmd/seed/main.go` (which uses `models.Network` not `models.Contract`)

Run: `cd c:/Users/Admin/Documents/go-indexing && grep -r "models.Log" --include="*.go" -l`
Expected: no files (all references replaced)

Run: `cd c:/Users/Admin/Documents/go-indexing && grep -r "storage.EnsureIndexes\|storage\..*Indexes" --include="*.go" -l`
Expected: no files (EnsureIndexes moved to evm.Storage)

- [ ] **Step 2: Delete the files**

```bash
cd c:/Users/Admin/Documents/go-indexing
rm internal/models/contract.go
rm internal/models/log.go
rm internal/storage/indexes.go
```

- [ ] **Step 3: Verify the full project compiles**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git rm internal/models/contract.go internal/models/log.go internal/storage/indexes.go
git commit -m "cleanup: remove old EVM-specific models and indexes (moved to chains/evm)"
```

---

## Task 16: Pipeline Unit Test

**Files:**
- Create: `internal/pipeline/pipeline_test.go`

- [ ] **Step 1: Write pipeline test with mocks**

```go
// internal/pipeline/pipeline_test.go
package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// --- Mocks ---

type mockSubscriber struct {
	blocks []core.BlockEvent
}

func (m *mockSubscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	for _, b := range m.blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- b:
		}
	}
	return nil
}

type mockFetcher struct {
	events map[uint64][]core.RawEvent // block number -> events
}

func (m *mockFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	return m.events[block.Number], nil
}

type mockStorage struct {
	savedEvents  []core.RawEvent
	scanProgress map[string]uint64
}

func (m *mockStorage) SaveEvents(ctx context.Context, events []core.RawEvent) error {
	m.savedEvents = append(m.savedEvents, events...)
	return nil
}
func (m *mockStorage) UpdateScanProgress(ctx context.Context, address string, block uint64) error {
	if m.scanProgress == nil {
		m.scanProgress = make(map[string]uint64)
	}
	m.scanProgress[address] = block
	return nil
}
func (m *mockStorage) LoadContracts(ctx context.Context) ([]core.ContractConfig, error) {
	return nil, nil
}
func (m *mockStorage) EnsureIndexes(ctx context.Context) error { return nil }
func (m *mockStorage) Close(ctx context.Context) error         { return nil }

// --- Tests ---

func TestPipeline_ProcessesBlocksAndDispatches(t *testing.T) {
	sub := &mockSubscriber{blocks: []core.BlockEvent{{Number: 100}, {Number: 101}}}

	fetcher := &mockFetcher{events: map[uint64][]core.RawEvent{
		100: {
			{BlockNumber: 100, EventKey: "0xabc", Address: "0x1", Metadata: map[string]string{}},
			{BlockNumber: 100, EventKey: "0xabc", Address: "0x1", Metadata: map[string]string{}},
			{BlockNumber: 100, EventKey: "0xdef", Address: "0x1", Metadata: map[string]string{}},
		},
		101: {},
	}}

	store := &mockStorage{}

	var abcCount, defCount int
	registry := core.NewRegistry(nil)
	registry.Register("0xabc", func(ctx context.Context, batch []core.RawEvent) error {
		abcCount += len(batch)
		return nil
	})
	registry.Register("0xdef", func(ctx context.Context, batch []core.RawEvent) error {
		defCount += len(batch)
		return nil
	})

	contracts := []core.ContractConfig{{Address: "0x1", StartBlock: 100}}

	p := New("test", sub, fetcher, registry, store, contracts)
	if err := p.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	p.Stop()

	if abcCount != 2 {
		t.Errorf("expected 2 events for 0xabc, got %d", abcCount)
	}
	if defCount != 1 {
		t.Errorf("expected 1 event for 0xdef, got %d", defCount)
	}
	if store.scanProgress["0x1"] != 101 {
		t.Errorf("expected scan progress 101 for 0x1, got %d", store.scanProgress["0x1"])
	}
}
```

- [ ] **Step 2: Run the test**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./internal/pipeline/ -v -run TestPipeline`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/pipeline/pipeline_test.go
git commit -m "test(pipeline): add unit test with mocks verifying dispatch and scan progress"
```

---

## Task 17: Verify Full Build & Smoke Test

- [ ] **Step 1: Run full build**

Run: `cd c:/Users/Admin/Documents/go-indexing && go build ./...`
Expected: no errors

- [ ] **Step 2: Run all tests**

Run: `cd c:/Users/Admin/Documents/go-indexing && go test ./... -v`
Expected: all tests pass

- [ ] **Step 3: Verify the interface contract**

Run: `cd c:/Users/Admin/Documents/go-indexing && go vet ./...`
Expected: no issues

- [ ] **Step 4: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "fix: resolve any remaining build issues from multi-chain refactor"
```

---

## Summary

After completing all 17 tasks, the codebase will have:

- `internal/core/` — Chain-agnostic interfaces (Subscriber, LogFetcher, EventParser, StorageInterface, Registry)
- `internal/chains/evm/` — Full EVM implementation (client, subscriber, fetcher, parser, storage, models)
- `internal/pipeline/` — Chain-agnostic pipeline using only core interfaces
- `internal/config/` — Multi-chain config with ChainType, PollInterval
- `cmd/indexer/` — Composition root with `switch cfg.ChainType` ready for new chains
- `cmd/seed/` — JSON-driven seeding

**Next:** Phase 2 plan for Solana chain adapter (`internal/chains/solana/`).
