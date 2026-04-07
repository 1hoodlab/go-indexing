# Solana Network Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a full Solana network handler in `internal/chains/solana/` that plugs into the existing pipeline via the `core.Subscriber`, `core.LogFetcher`, `core.EventParser`, and `core.StorageInterface` interfaces.

**Architecture:** HTTP polling with `getSlot` + `getBlocksWithLimit` every 400ms. Events are extracted from `meta.logMessages` via `"Program data: <base64>"` lines scoped to target program invocations. Anchor IDL discriminator + borsh decoding with raw fallback.

**Tech Stack:** Go 1.25, `github.com/gagliardetto/solana-go` (Solana RPC + types), `go.mongodb.org/mongo-driver` (MongoDB), standard `crypto/sha256` for discriminator computation.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `internal/chains/solana/models.go` | Create | Program, ProgramLog, DecodedEvent BSON structs |
| `internal/chains/solana/client.go` | Create | Simplified Block/Transaction types + concrete RPC client wrapping gagliardetto |
| `internal/chains/solana/rpcpool.go` | Create | SolanaRPCClient interface + multi-URL fallback pool |
| `internal/chains/solana/rpcpool_test.go` | Create | Pool rotation + permanent removal tests |
| `internal/chains/solana/subscriber.go` | Create | core.Subscriber: polls getSlot + getBlocksWithLimit |
| `internal/chains/solana/subscriber_test.go` | Create | Backfill, live poll, skip-empty-slots tests |
| `internal/chains/solana/fetcher.go` | Create | core.LogFetcher: getBlock + log scope extraction + RawEvent output |
| `internal/chains/solana/fetcher_test.go` | Create | Scope tracking, multi-program, versioned tx account merging tests |
| `internal/chains/solana/borsh.go` | Create | Dynamic borsh decoder driven by IDL field schemas |
| `internal/chains/solana/borsh_test.go` | Create | Primitive, publicKey, string, vec, option, nested struct tests |
| `internal/chains/solana/parser.go` | Create | core.EventParser: discriminator lookup + borsh decode + raw fallback |
| `internal/chains/solana/parser_test.go` | Create | IDL path + unknown discriminator fallback tests |
| `internal/chains/solana/storage.go` | Create | core.StorageInterface: program_logs + decoded_events + programs collections |
| `internal/chains/solana/storage_test.go` | Create | SaveEvents, UpdateScanProgress, LoadContracts, index creation tests |
| `internal/chains/solana/handler.go` | Create | Solana default handler: save raw + decode + save decoded |
| `cmd/indexer/main.go` | Modify | Add `case "solana": runSolana(cfg)` |
| `cmd/seed/data/solana-devnet.json` | Create | Example seed config for Solana devnet |
| `go.mod` / `go.sum` | Modify | Add `github.com/gagliardetto/solana-go` dependency |

---

## Task 1: Add Dependency

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add gagliardetto/solana-go**

```bash
cd c:/Users/Admin/Documents/go-indexing
go get github.com/gagliardetto/solana-go@latest
```

Expected: `go.mod` now contains `github.com/gagliardetto/solana-go vX.X.X`

- [ ] **Step 2: Verify module downloads**

```bash
go mod tidy
```

Expected: no errors, `go.sum` updated.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore(deps): add gagliardetto/solana-go"
```

---

## Task 2: Models

**Files:**
- Create: `internal/chains/solana/models.go`

- [ ] **Step 1: Create models**

Create `internal/chains/solana/models.go`:

```go
package solana

import (
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Program is a Solana program to index (equivalent to EVM Contract).
type Program struct {
	ID              primitive.ObjectID   `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID   `bson:"network_id"`
	Address         string               `bson:"address"`          // base58 pubkey
	IDL             string               `bson:"idl,omitempty"`    // Anchor IDL JSON
	Events          []string             `bson:"events"`           // event names to watch
	StartSlot       uint64               `bson:"start_slot"`
	LastScannedSlot uint64               `bson:"last_scanned_slot"`
	Status          models.NetworkStatus `bson:"status"`
	CreatedAt       time.Time            `bson:"created_at"`
	UpdatedAt       time.Time            `bson:"updated_at"`
}

// ProgramLog is a raw captured program event (equivalent to EVM Log).
type ProgramLog struct {
	ID              primitive.ObjectID `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID `bson:"network_id"`
	ProgramID       primitive.ObjectID `bson:"program_id"`
	Slot            uint64             `bson:"slot"`
	BlockHash       string             `bson:"block_hash"`
	TransactionHash string             `bson:"transaction_hash"` // base58 signature
	LogIndex        int                `bson:"log_index"`
	ProgramAddress  string             `bson:"program_address"` // base58 pubkey
	Discriminator   string             `bson:"discriminator"`   // hex of first 8 bytes
	EventKey        string             `bson:"event_key"`       // "programAddress:discriminatorHex"
	RawData         string             `bson:"raw_data"`        // base64 original bytes
	AccountKeys     []string           `bson:"account_keys"`    // all accounts in tx
	CreatedAt       time.Time          `bson:"created_at"`
}

// DecodedEvent stores a decoded program event.
type DecodedEvent struct {
	ID              primitive.ObjectID     `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ProgramAddress  string                 `bson:"program_address"`
	EventName       string                 `bson:"event_name"` // from IDL, or discriminatorHex
	Slot            uint64                 `bson:"slot"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        int                    `bson:"log_index"`
	Data            map[string]interface{} `bson:"data"`
	CreatedAt       time.Time              `bson:"created_at"`
}
```

- [ ] **Step 2: Verify compilation**

```bash
cd c:/Users/Admin/Documents/go-indexing
go build ./internal/chains/solana/...
```

Expected: no errors (empty package is fine at this point).

- [ ] **Step 3: Commit**

```bash
git add internal/chains/solana/models.go
git commit -m "feat(solana): add Program, ProgramLog, DecodedEvent models"
```

---

## Task 3: RPC Client + Pool

**Files:**
- Create: `internal/chains/solana/client.go`
- Create: `internal/chains/solana/rpcpool.go`
- Create: `internal/chains/solana/rpcpool_test.go`

- [ ] **Step 1: Write the failing pool tests**

Create `internal/chains/solana/rpcpool_test.go`:

```go
package solana

import (
	"context"
	"errors"
	"testing"
)

var errFakeRPC = errors.New("fake rpc error")

type mockSolanaClient struct {
	slot uint64
	fail bool
}

func (m *mockSolanaClient) GetSlot(ctx context.Context) (uint64, error) {
	if m.fail {
		return 0, errFakeRPC
	}
	return m.slot, nil
}

func (m *mockSolanaClient) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	if m.fail {
		return nil, errFakeRPC
	}
	slots := make([]uint64, 0, limit)
	for i := uint64(0); i < limit; i++ {
		slots = append(slots, startSlot+i)
	}
	return slots, nil
}

func (m *mockSolanaClient) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	if m.fail {
		return nil, errFakeRPC
	}
	return &Block{Slot: slot, Blockhash: "testhash"}, nil
}

func TestRPCPool_UsesFirstURL(t *testing.T) {
	good := &mockSolanaClient{slot: 100}
	dialFn := func(url string) (SolanaRPCClient, error) {
		if url == "http://good" {
			return good, nil
		}
		return nil, errFakeRPC
	}

	pool, err := NewRPCPoolWithDialFn([]string{"http://good", "http://bad"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slot, err := pool.GetSlot(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot != 100 {
		t.Fatalf("expected 100, got %d", slot)
	}
}

func TestRPCPool_RotatesOnError(t *testing.T) {
	bad := &mockSolanaClient{fail: true}
	good := &mockSolanaClient{slot: 200}
	calls := 0
	dialFn := func(url string) (SolanaRPCClient, error) {
		calls++
		if url == "http://bad" {
			return bad, nil
		}
		return good, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"http://bad", "http://good"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slot, err := pool.GetSlot(context.Background())
	if err != nil {
		t.Fatalf("expected rotation to succeed: %v", err)
	}
	if slot != 200 {
		t.Fatalf("expected 200 after rotation, got %d", slot)
	}
}

func TestRPCPool_ErrorsWhenAllFail(t *testing.T) {
	dialFn := func(url string) (SolanaRPCClient, error) {
		return &mockSolanaClient{fail: true}, nil
	}
	pool, err := NewRPCPoolWithDialFn([]string{"http://a", "http://b"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	_, err = pool.GetSlot(context.Background())
	if !errors.Is(err, ErrAllRPCsFailed) {
		t.Fatalf("expected ErrAllRPCsFailed, got %v", err)
	}
}

func TestRPCPool_NoURLsErrors(t *testing.T) {
	_, err := NewRPCPoolWithDialFn([]string{}, func(url string) (SolanaRPCClient, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrNoRPCURLs) {
		t.Fatalf("expected ErrNoRPCURLs, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cd c:/Users/Admin/Documents/go-indexing
go test ./internal/chains/solana/... 2>&1 | head -20
```

Expected: compile errors (types not defined yet).

- [ ] **Step 3: Create `client.go` with Block types and concrete client**

Create `internal/chains/solana/client.go`:

```go
package solana

import (
	"context"

	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// Block is a simplified Solana block for indexing.
type Block struct {
	Slot         uint64
	Blockhash    string
	BlockTime    int64 // Unix timestamp
	Transactions []Transaction
}

// Transaction holds the indexing-relevant fields from a Solana transaction.
type Transaction struct {
	Signature   string   // base58 first signature
	AccountKeys []string // all account keys (message + loaded addresses)
	LogMessages []string // meta.logMessages
}

// Client wraps a gagliardetto RPC client with confirmed commitment baked in.
type Client struct {
	rpc *solanarpc.Client
}

// NewClient creates a Client connected to the given RPC URL.
func NewClient(url string) (*Client, error) {
	return &Client{rpc: solanarpc.New(url)}, nil
}

// GetSlot returns the current confirmed slot.
func (c *Client) GetSlot(ctx context.Context) (uint64, error) {
	commitment := solanarpc.CommitmentConfirmed
	return c.rpc.GetSlot(ctx, solanarpc.GetSlotOpts{Commitment: &commitment})
}

// GetBlocksWithLimit returns up to limit confirmed block slot numbers starting at startSlot.
func (c *Client) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	result, err := c.rpc.GetBlocksWithLimit(ctx, startSlot, limit, solanarpc.CommitmentConfirmed)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return []uint64(*result), nil
}

// GetBlock fetches a full block with transaction details.
// Returns nil, nil if the slot has no block.
func (c *Client) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	commitment := solanarpc.CommitmentConfirmed
	maxVersion := uint64(0)
	noRewards := false

	result, err := c.rpc.GetBlockWithOpts(ctx, slot, &solanarpc.GetBlockOpts{
		Commitment:                     &commitment,
		Encoding:                       solanago.EncodingJSON,
		TransactionDetails:             solanarpc.TransactionDetailsFull,
		MaxSupportedTransactionVersion: &maxVersion,
		Rewards:                        &noRewards,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	block := &Block{
		Slot:      slot,
		Blockhash: result.Blockhash.String(),
	}
	if result.BlockTime != nil {
		block.BlockTime = int64(*result.BlockTime)
	}

	for _, txWithMeta := range result.Transactions {
		if txWithMeta.Transaction == nil || txWithMeta.Meta == nil {
			continue
		}
		tx := txWithMeta.Transaction
		if len(tx.Signatures) == 0 {
			continue
		}

		// Merge message account keys + loaded addresses (versioned tx support)
		var accounts []string
		for _, key := range tx.Message.AccountKeys {
			accounts = append(accounts, key.String())
		}
		for _, key := range txWithMeta.Meta.LoadedAddresses.Writable {
			accounts = append(accounts, key.String())
		}
		for _, key := range txWithMeta.Meta.LoadedAddresses.Readonly {
			accounts = append(accounts, key.String())
		}

		block.Transactions = append(block.Transactions, Transaction{
			Signature:   tx.Signatures[0].String(),
			AccountKeys: accounts,
			LogMessages: txWithMeta.Meta.LogMessages,
		})
	}

	return block, nil
}
```

- [ ] **Step 4: Create `rpcpool.go`**

Create `internal/chains/solana/rpcpool.go`:

```go
package solana

import (
	"context"
	"errors"
	"log"
	"sync"
)

var (
	ErrNoRPCURLs     = errors.New("solana: no RPC URLs provided")
	ErrAllRPCsFailed = errors.New("solana: all RPC URLs failed")
)

// SolanaRPCClient is the interface for Solana RPC calls.
type SolanaRPCClient interface {
	GetSlot(ctx context.Context) (uint64, error)
	GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error)
	GetBlock(ctx context.Context, slot uint64) (*Block, error)
}

// DialFn creates a SolanaRPCClient from a URL. Injected for testing.
type DialFn func(url string) (SolanaRPCClient, error)

// DefaultDialFn connects to a real Solana RPC endpoint.
func DefaultDialFn(url string) (SolanaRPCClient, error) {
	return NewClient(url)
}

// RPCPool wraps multiple Solana RPC URLs with automatic fallback.
// Failed URLs are permanently removed (restart to reset).
type RPCPool struct {
	mu        sync.RWMutex
	active    SolanaRPCClient
	activeURL string
	remaining []string
	dialFn    DialFn
}

// NewRPCPool creates a pool from a list of RPC URLs.
func NewRPCPool(urls []string) (*RPCPool, error) {
	return NewRPCPoolWithDialFn(urls, DefaultDialFn)
}

// NewRPCPoolWithDialFn creates a pool with a custom dial function (for testing).
func NewRPCPoolWithDialFn(urls []string, dialFn DialFn) (*RPCPool, error) {
	if len(urls) == 0 {
		return nil, ErrNoRPCURLs
	}
	pool := &RPCPool{dialFn: dialFn}
	for i, url := range urls {
		client, err := dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed to connect: %v", url, err)
			continue
		}
		pool.active = client
		pool.activeURL = url
		pool.remaining = make([]string, len(urls[i+1:]))
		copy(pool.remaining, urls[i+1:])
		return pool, nil
	}
	return nil, ErrAllRPCsFailed
}

func (p *RPCPool) rotate() error {
	for len(p.remaining) > 0 {
		url := p.remaining[0]
		p.remaining = p.remaining[1:]
		client, err := p.dialFn(url)
		if err != nil {
			log.Printf("solana rpc pool: %s failed: %v", url, err)
			continue
		}
		p.active = client
		p.activeURL = url
		log.Printf("solana rpc pool: switched to %s", url)
		return nil
	}
	return ErrAllRPCsFailed
}

func callWithFallback[T any](p *RPCPool, fn func(SolanaRPCClient) (T, error)) (T, error) {
	p.mu.RLock()
	client := p.active
	p.mu.RUnlock()

	result, err := fn(client)
	if err == nil {
		return result, nil
	}
	for {
		p.mu.Lock()
		if rotateErr := p.rotate(); rotateErr != nil {
			p.mu.Unlock()
			var zero T
			return zero, ErrAllRPCsFailed
		}
		client = p.active
		p.mu.Unlock()

		result, err = fn(client)
		if err == nil {
			return result, nil
		}
	}
}

func (p *RPCPool) GetSlot(ctx context.Context) (uint64, error) {
	return callWithFallback(p, func(c SolanaRPCClient) (uint64, error) {
		return c.GetSlot(ctx)
	})
}

func (p *RPCPool) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	return callWithFallback(p, func(c SolanaRPCClient) ([]uint64, error) {
		return c.GetBlocksWithLimit(ctx, startSlot, limit)
	})
}

func (p *RPCPool) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	return callWithFallback(p, func(c SolanaRPCClient) (*Block, error) {
		return c.GetBlock(ctx, slot)
	})
}

// ActiveURL returns the currently active RPC URL.
func (p *RPCPool) ActiveURL() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeURL
}
```

- [ ] **Step 5: Run tests — expect pass**

```bash
cd c:/Users/Admin/Documents/go-indexing
go test ./internal/chains/solana/... -run TestRPCPool -v
```

Expected:
```
--- PASS: TestRPCPool_UsesFirstURL
--- PASS: TestRPCPool_RotatesOnError
--- PASS: TestRPCPool_ErrorsWhenAllFail
--- PASS: TestRPCPool_NoURLsErrors
PASS
```

- [ ] **Step 6: Commit**

```bash
git add internal/chains/solana/client.go internal/chains/solana/rpcpool.go internal/chains/solana/rpcpool_test.go
git commit -m "feat(solana): add RPC client, pool with fallback"
```

---

## Task 4: Subscriber

**Files:**
- Create: `internal/chains/solana/subscriber.go`
- Create: `internal/chains/solana/subscriber_test.go`

- [ ] **Step 1: Write failing subscriber tests**

Create `internal/chains/solana/subscriber_test.go`:

```go
package solana

import (
	"context"
	"testing"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// mockRPC simulates getSlot and getBlocksWithLimit for subscriber tests.
type mockRPC struct {
	slots    []uint64 // sequence of values returned by GetSlot calls
	slotIdx  int
	// blocks maps startSlot → confirmed slot list
	blocks map[uint64][]uint64
}

func (m *mockRPC) GetSlot(ctx context.Context) (uint64, error) {
	if m.slotIdx >= len(m.slots) {
		return m.slots[len(m.slots)-1], nil
	}
	s := m.slots[m.slotIdx]
	m.slotIdx++
	return s, nil
}

func (m *mockRPC) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	if slots, ok := m.blocks[startSlot]; ok {
		return slots, nil
	}
	return nil, nil
}

func (m *mockRPC) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	return &Block{Slot: slot, Blockhash: "hash", BlockTime: 1000}, nil
}

func collectEvents(out <-chan core.BlockEvent, n int, timeout time.Duration) []core.BlockEvent {
	var events []core.BlockEvent
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(events) < n {
		select {
		case ev, ok := <-out:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-timer.C:
			return events
		}
	}
	return events
}

func TestSubscriber_BackfillsFromSlot(t *testing.T) {
	rpc := &mockRPC{
		slots: []uint64{105, 105},
		blocks: map[uint64][]uint64{
			100: {100, 101, 103, 104, 105}, // slot 102 has no block
		},
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 100, out) //nolint:errcheck

	events := collectEvents(out, 5, 500*time.Millisecond)
	cancel()

	if len(events) < 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	slots := make([]uint64, len(events))
	for i, ev := range events {
		slots[i] = ev.Number
	}
	expected := []uint64{100, 101, 103, 104, 105}
	for i, want := range expected {
		if slots[i] != want {
			t.Errorf("event[%d]: want slot %d, got %d", i, want, slots[i])
		}
	}
}

func TestSubscriber_SkipsEmptySlots(t *testing.T) {
	// getBlocksWithLimit returns only confirmed blocks, skipping empty slots
	rpc := &mockRPC{
		slots:  []uint64{200},
		blocks: map[uint64][]uint64{150: {150, 152, 155}}, // 151, 153, 154 have no blocks
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 150, out) //nolint:errcheck

	events := collectEvents(out, 3, 500*time.Millisecond)
	cancel()

	if len(events) != 3 {
		t.Fatalf("expected 3 events (skip empty slots), got %d", len(events))
	}
	for i, wantSlot := range []uint64{150, 152, 155} {
		if events[i].Number != wantSlot {
			t.Errorf("event[%d]: want %d, got %d", i, wantSlot, events[i].Number)
		}
	}
}

func TestSubscriber_SetsBlockHash(t *testing.T) {
	rpc := &mockRPC{
		slots:  []uint64{10},
		blocks: map[uint64][]uint64{10: {10}},
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 10, out) //nolint:errcheck

	events := collectEvents(out, 1, 500*time.Millisecond)
	cancel()

	if len(events) == 0 {
		t.Fatal("no events received")
	}
	if events[0].Hash == "" {
		t.Error("expected non-empty block hash")
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestSubscriber 2>&1 | head -10
```

Expected: compile error (`NewSubscriber` not defined).

- [ ] **Step 3: Implement `subscriber.go`**

Create `internal/chains/solana/subscriber.go`:

```go
package solana

import (
	"context"
	"log"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

const backfillBatchSize = uint64(100)

// Subscriber implements core.Subscriber for Solana via HTTP polling.
type Subscriber struct {
	client       SolanaRPCClient
	pollInterval time.Duration
}

// NewSubscriber creates a Subscriber with the given RPC client and poll interval.
func NewSubscriber(client SolanaRPCClient, pollInterval time.Duration) *Subscriber {
	return &Subscriber{client: client, pollInterval: pollInterval}
}

// SubscribeBlocks polls for new confirmed slots and sends BlockEvents to out.
// from is the first slot to process (backfill start point).
func (s *Subscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	lastProcessed := from - 1
	if from == 0 {
		// No backfill requested — start from current tip
		slot, err := s.client.GetSlot(ctx)
		if err != nil {
			return err
		}
		lastProcessed = slot
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentSlot, err := s.client.GetSlot(ctx)
		if err != nil {
			log.Printf("solana subscriber: getSlot error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		if currentSlot <= lastProcessed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		// Fetch confirmed block slots in batches (skips empty slots)
		startSlot := lastProcessed + 1
		limit := currentSlot - lastProcessed
		if limit > backfillBatchSize {
			limit = backfillBatchSize
		}

		confirmedSlots, err := s.client.GetBlocksWithLimit(ctx, startSlot, limit)
		if err != nil {
			log.Printf("solana subscriber: getBlocksWithLimit error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.pollInterval):
			}
			continue
		}

		for _, slot := range confirmedSlots {
			block, err := s.client.GetBlock(ctx, slot)
			if err != nil {
				log.Printf("solana subscriber: getBlock(%d) error: %v", slot, err)
				continue
			}
			if block == nil {
				continue // empty slot
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- core.BlockEvent{
				Number:    slot,
				Hash:      block.Blockhash,
				Timestamp: uint64(block.BlockTime),
			}:
			}
			lastProcessed = slot
		}

		if len(confirmedSlots) == 0 {
			lastProcessed = currentSlot
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}
}
```

- [ ] **Step 4: Run tests — expect pass**

```bash
go test ./internal/chains/solana/... -run TestSubscriber -v
```

Expected:
```
--- PASS: TestSubscriber_BackfillsFromSlot
--- PASS: TestSubscriber_SkipsEmptySlots
--- PASS: TestSubscriber_SetsBlockHash
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/subscriber.go internal/chains/solana/subscriber_test.go
git commit -m "feat(solana): add Subscriber with slot polling and backfill"
```

---

## Task 5: Fetcher — Log Scope Extraction

**Files:**
- Create: `internal/chains/solana/fetcher.go` (partial — `extractProgramEvents` only)
- Create: `internal/chains/solana/fetcher_test.go` (scope extraction tests)

- [ ] **Step 1: Write failing tests for log scope extraction**

Create `internal/chains/solana/fetcher_test.go`:

```go
package solana

import (
	"testing"
)

func TestExtractProgramEvents_BasicScope(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].logIndex != 1 {
		t.Errorf("expected logIndex 1, got %d", events[0].logIndex)
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_IgnoresOtherPrograms(t *testing.T) {
	logs := []string{
		"Program OtherProg invoke [1]",
		"Program data: AAAA",
		"Program OtherProg success",
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event (only from TokenkegQ), got %d", len(events))
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_NestedCPI(t *testing.T) {
	// Target program calls another program via CPI; only capture outer program's data
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==", // ← TokenkegQ's event
		"Program InnerProg invoke [2]",
		"Program data: ZZZZ",    // ← InnerProg's event, should be ignored
		"Program InnerProg success",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event (outer program only), got %d: %+v", len(events), events)
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_MultipleEventsInOneTx(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AAAA",
		"Program data: BBBB",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].dataBase64 != "AAAA" || events[1].dataBase64 != "BBBB" {
		t.Errorf("unexpected data: %+v", events)
	}
}

func TestExtractProgramEvents_NoEvents(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program log: some message",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestExtract 2>&1 | head -10
```

Expected: compile error (`extractProgramEvents` not defined).

- [ ] **Step 3: Implement `extractProgramEvents` in `fetcher.go`**

Create `internal/chains/solana/fetcher.go`:

```go
package solana

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// programEvent is a raw event extracted from a transaction's logMessages.
type programEvent struct {
	logIndex   int
	dataBase64 string
}

// extractProgramEvents scans logMessages for "Program data:" lines emitted
// within the invocation scope of targetProgram (depth 1 only — not CPIs).
func extractProgramEvents(targetProgram string, logMessages []string) []programEvent {
	var events []programEvent
	inScope := false

	for i, line := range logMessages {
		switch {
		case strings.HasPrefix(line, "Program "+targetProgram+" invoke ["):
			// Only capture at depth 1 (direct invocation)
			depthStr := strings.TrimPrefix(line, "Program "+targetProgram+" invoke [")
			depthStr = strings.TrimSuffix(depthStr, "]")
			if depth, err := strconv.Atoi(depthStr); err == nil && depth == 1 {
				inScope = true
			}
		case line == "Program "+targetProgram+" success" ||
			line == "Program "+targetProgram+" failed":
			inScope = false
		case inScope && strings.HasPrefix(line, "Program data: "):
			data := strings.TrimPrefix(line, "Program data: ")
			events = append(events, programEvent{logIndex: i, dataBase64: data})
		}
	}

	return events
}
```

- [ ] **Step 4: Run tests — expect pass**

```bash
go test ./internal/chains/solana/... -run TestExtract -v
```

Expected:
```
--- PASS: TestExtractProgramEvents_BasicScope
--- PASS: TestExtractProgramEvents_IgnoresOtherPrograms
--- PASS: TestExtractProgramEvents_NestedCPI
--- PASS: TestExtractProgramEvents_MultipleEventsInOneTx
--- PASS: TestExtractProgramEvents_NoEvents
PASS
```

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/fetcher.go internal/chains/solana/fetcher_test.go
git commit -m "feat(solana): add log scope extraction for program events"
```

---

## Task 6: Fetcher — Full FetchLogs

**Files:**
- Modify: `internal/chains/solana/fetcher.go` (add LogFetcher struct + FetchLogs)
- Modify: `internal/chains/solana/fetcher_test.go` (add FetchLogs tests)

- [ ] **Step 1: Add FetchLogs tests to `fetcher_test.go`**

Append to `internal/chains/solana/fetcher_test.go`:

```go
func makeMockBlock(programID string, logs []string) Block {
	return Block{
		Slot:      500,
		Blockhash: "blockhash1",
		BlockTime: 1700000000,
		Transactions: []Transaction{
			{
				Signature:   "sig1",
				AccountKeys: []string{programID, "user1"},
				LogMessages: logs,
			},
		},
	}
}

func TestLogFetcher_ExtractsRawEvents(t *testing.T) {
	programID := "TokenkegQ"
	block := makeMockBlock(programID, []string{
		"Program " + programID + " invoke [1]",
		"Program data: AQIDBA==",
		"Program " + programID + " success",
	})

	fetcher := NewLogFetcher(nil, "netid123", map[string]programInfo{
		programID: {id: "progid1", idl: ""},
	})

	contracts := []core.ContractConfig{
		{Address: programID, StartBlock: 100, LastScanned: 400},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.Address != programID {
		t.Errorf("expected address %s, got %s", programID, ev.Address)
	}
	if ev.BlockNumber != 500 {
		t.Errorf("expected block 500, got %d", ev.BlockNumber)
	}
	if ev.TransactionHash != "sig1" {
		t.Errorf("expected sig1, got %s", ev.TransactionHash)
	}
	// EventKey = "programID:discriminatorHex"
	if !strings.HasPrefix(ev.EventKey, programID+":") {
		t.Errorf("unexpected EventKey format: %s", ev.EventKey)
	}
	if len(ev.RawData) == 0 {
		t.Error("expected non-empty RawData")
	}
	if ev.Metadata["network_id"] != "netid123" {
		t.Errorf("expected network_id netid123, got %s", ev.Metadata["network_id"])
	}
}

func TestLogFetcher_SkipsProgramNotInContracts(t *testing.T) {
	block := makeMockBlock("OtherProg", []string{
		"Program OtherProg invoke [1]",
		"Program data: AQIDBA==",
		"Program OtherProg success",
	})

	fetcher := NewLogFetcher(nil, "netid", map[string]programInfo{
		"TokenkegQ": {id: "progid1"},
	})
	contracts := []core.ContractConfig{
		{Address: "TokenkegQ", StartBlock: 100},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events (program not in contracts), got %d", len(events))
	}
}

func TestLogFetcher_SkipsSlotBeforeStartBlock(t *testing.T) {
	programID := "TokenkegQ"
	block := makeMockBlock(programID, []string{
		"Program " + programID + " invoke [1]",
		"Program data: AQIDBA==",
		"Program " + programID + " success",
	})
	block.Slot = 50 // before StartBlock

	fetcher := NewLogFetcher(nil, "netid", map[string]programInfo{
		programID: {id: "progid1"},
	})
	contracts := []core.ContractConfig{
		{Address: programID, StartBlock: 100},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events (slot before start), got %d", len(events))
	}
}
```

- [ ] **Step 2: Run new tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestLogFetcher 2>&1 | head -10
```

Expected: compile error (`NewLogFetcher`, `fetchLogsFromBlock` not defined).

- [ ] **Step 3: Complete `fetcher.go` with LogFetcher**

Append to `internal/chains/solana/fetcher.go` (after `extractProgramEvents`):

```go
// programInfo holds the MongoDB ID and IDL for a known program.
type programInfo struct {
	id  string
	idl string
}

// LogFetcher implements core.LogFetcher for Solana.
type LogFetcher struct {
	client      SolanaRPCClient
	networkID   string
	programMap  map[string]programInfo // key: base58 program address
}

// NewLogFetcher creates a LogFetcher.
// programMap maps base58 program address → programInfo.
func NewLogFetcher(client SolanaRPCClient, networkID string, programMap map[string]programInfo) *LogFetcher {
	return &LogFetcher{client: client, networkID: networkID, programMap: programMap}
}

// FetchLogs implements core.LogFetcher.
func (f *LogFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	b, err := f.client.GetBlock(ctx, block.Number)
	if err != nil {
		return nil, fmt.Errorf("solana fetcher: getBlock(%d): %w", block.Number, err)
	}
	if b == nil {
		return nil, nil // empty slot
	}
	return f.fetchLogsFromBlock(ctx, *b, contracts)
}

// fetchLogsFromBlock extracts RawEvents from an already-fetched Block.
// Separated for unit testing without a real RPC client.
func (f *LogFetcher) fetchLogsFromBlock(_ context.Context, b Block, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	// Build a map of active program addresses for this slot
	activePrograms := make(map[string]core.ContractConfig)
	for _, c := range contracts {
		nextSlot := c.StartBlock
		if c.LastScanned > 0 {
			nextSlot = c.LastScanned + 1
		}
		if b.Slot >= nextSlot {
			activePrograms[c.Address] = c
		}
	}
	if len(activePrograms) == 0 {
		return nil, nil
	}

	var events []core.RawEvent

	for _, tx := range b.Transactions {
		// Build lookup set from this tx's account keys
		accountSet := make(map[string]struct{}, len(tx.AccountKeys))
		for _, k := range tx.AccountKeys {
			accountSet[k] = struct{}{}
		}
		accountKeysJSON, _ := json.Marshal(tx.AccountKeys)

		for programAddr := range activePrograms {
			if _, mentionedInTx := accountSet[programAddr]; !mentionedInTx {
				continue
			}

			pi, known := f.programMap[programAddr]
			if !known {
				continue
			}

			programEvents := extractProgramEvents(programAddr, tx.LogMessages)
			for _, pe := range programEvents {
				rawBytes, err := base64.StdEncoding.DecodeString(pe.dataBase64)
				if err != nil {
					continue // malformed base64 — skip
				}
				if len(rawBytes) < 8 {
					continue // not enough bytes for a discriminator
				}

				discriminatorHex := hex.EncodeToString(rawBytes[:8])
				eventKey := programAddr + ":" + discriminatorHex

				events = append(events, core.RawEvent{
					BlockNumber:     b.Slot,
					TransactionHash: tx.Signature,
					Address:         programAddr,
					EventKey:        eventKey,
					RawData:         rawBytes,
					Metadata: map[string]string{
						"network_id":   f.networkID,
						"program_id":   pi.id,
						"block_hash":   b.Blockhash,
						"slot":         strconv.FormatUint(b.Slot, 10),
						"log_index":    strconv.Itoa(pe.logIndex),
						"account_keys": string(accountKeysJSON),
						"idl":          pi.idl,
					},
				})
			}
		}
	}

	return events, nil
}
```

- [ ] **Step 4: Run all fetcher tests — expect pass**

```bash
go test ./internal/chains/solana/... -run "TestExtract|TestLogFetcher" -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/fetcher.go internal/chains/solana/fetcher_test.go
git commit -m "feat(solana): add LogFetcher with program scope extraction"
```

---

## Task 7: Borsh Decoder

**Files:**
- Create: `internal/chains/solana/borsh.go`
- Create: `internal/chains/solana/borsh_test.go`

- [ ] **Step 1: Write failing borsh tests**

Create `internal/chains/solana/borsh_test.go`:

```go
package solana

import (
	"encoding/binary"
	"math"
	"testing"
)

func makeIDLField(name, typ string) idlField {
	return idlField{Name: name, Type: typ}
}

func TestDecodeBorsh_Primitives(t *testing.T) {
	// u8=42, u16=1000, u32=70000, u64=1234567890, bool=true
	buf := make([]byte, 1+2+4+8+1)
	buf[0] = 42
	binary.LittleEndian.PutUint16(buf[1:], 1000)
	binary.LittleEndian.PutUint32(buf[3:], 70000)
	binary.LittleEndian.PutUint64(buf[7:], 1234567890)
	buf[15] = 1

	fields := []idlField{
		makeIDLField("a", "u8"),
		makeIDLField("b", "u16"),
		makeIDLField("c", "u32"),
		makeIDLField("d", "u64"),
		makeIDLField("e", "bool"),
	}

	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["a"] != uint64(42) {
		t.Errorf("u8: expected 42, got %v", result["a"])
	}
	if result["b"] != uint64(1000) {
		t.Errorf("u16: expected 1000, got %v", result["b"])
	}
	if result["c"] != uint64(70000) {
		t.Errorf("u32: expected 70000, got %v", result["c"])
	}
	if result["d"] != uint64(1234567890) {
		t.Errorf("u64: expected 1234567890, got %v", result["d"])
	}
	if result["e"] != true {
		t.Errorf("bool: expected true, got %v", result["e"])
	}
}

func TestDecodeBorsh_PublicKey(t *testing.T) {
	// 32 zero bytes → "11111111111111111111111111111111" in base58
	buf := make([]byte, 32)
	fields := []idlField{makeIDLField("pk", "publicKey")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pk, ok := result["pk"].(string)
	if !ok || pk == "" {
		t.Errorf("expected non-empty base58 string, got %v", result["pk"])
	}
}

func TestDecodeBorsh_String(t *testing.T) {
	text := "hello"
	buf := make([]byte, 4+len(text))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(text)))
	copy(buf[4:], text)

	fields := []idlField{makeIDLField("msg", "string")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "hello" {
		t.Errorf("expected 'hello', got %v", result["msg"])
	}
}

func TestDecodeBorsh_Vec(t *testing.T) {
	// vec<u32> with 3 elements: [1, 2, 3]
	buf := make([]byte, 4+3*4)
	binary.LittleEndian.PutUint32(buf[:4], 3)
	binary.LittleEndian.PutUint32(buf[4:], 1)
	binary.LittleEndian.PutUint32(buf[8:], 2)
	binary.LittleEndian.PutUint32(buf[12:], 3)

	fields := []idlField{{Name: "nums", Type: map[string]interface{}{"vec": "u32"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	nums, ok := result["nums"].([]interface{})
	if !ok || len(nums) != 3 {
		t.Fatalf("expected []interface{} len 3, got %T %v", result["nums"], result["nums"])
	}
	if nums[0] != uint64(1) || nums[1] != uint64(2) || nums[2] != uint64(3) {
		t.Errorf("unexpected values: %v", nums)
	}
}

func TestDecodeBorsh_OptionSome(t *testing.T) {
	// option<u64> = Some(99)
	buf := make([]byte, 1+8)
	buf[0] = 1 // Some
	binary.LittleEndian.PutUint64(buf[1:], 99)

	fields := []idlField{{Name: "val", Type: map[string]interface{}{"option": "u64"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["val"] != uint64(99) {
		t.Errorf("expected 99, got %v", result["val"])
	}
}

func TestDecodeBorsh_OptionNone(t *testing.T) {
	buf := []byte{0} // None
	fields := []idlField{{Name: "val", Type: map[string]interface{}{"option": "u64"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["val"] != nil {
		t.Errorf("expected nil for None, got %v", result["val"])
	}
}

func TestDecodeBorsh_InsufficientData(t *testing.T) {
	buf := []byte{1} // only 1 byte, u64 needs 8
	fields := []idlField{makeIDLField("x", "u64")}
	_, err := decodeBorsh(buf, fields, nil)
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}
}

func TestDecodeBorsh_F64(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(3.14))
	fields := []idlField{makeIDLField("pi", "f64")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["pi"] != 3.14 {
		t.Errorf("expected 3.14, got %v", result["pi"])
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestDecodeBorsh 2>&1 | head -10
```

Expected: compile error (`decodeBorsh` not defined).

- [ ] **Step 3: Implement `borsh.go`**

Create `internal/chains/solana/borsh.go`:

```go
package solana

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"

	solanago "github.com/gagliardetto/solana-go"
)

// idlField describes a single field in an Anchor IDL event or struct.
// Type can be a string ("u64", "publicKey", etc.) or a map for complex types.
type idlField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"` // string | map[string]interface{}
}

// idlEvent describes an Anchor IDL event.
type idlEvent struct {
	Name   string     `json:"name"`
	Fields []idlField `json:"fields"`
}

// idlTypeDef is a named struct definition in the IDL types section.
type idlTypeDef struct {
	Name string `json:"name"`
	Type struct {
		Kind   string     `json:"kind"`
		Fields []idlField `json:"fields"`
	} `json:"type"`
}

// anchorIDL is the minimal parsed representation of an Anchor IDL JSON.
type anchorIDL struct {
	Events []idlEvent   `json:"events"`
	Types  []idlTypeDef `json:"types"`
}

// parseIDL parses an Anchor IDL JSON string into anchorIDL.
func parseIDL(idlJSON string) (*anchorIDL, error) {
	var idl anchorIDL
	if err := json.Unmarshal([]byte(idlJSON), &idl); err != nil {
		return nil, fmt.Errorf("parseIDL: %w", err)
	}
	return &idl, nil
}

// buildTypeMap builds a lookup from type name → idlTypeDef.
func buildTypeMap(idl *anchorIDL) map[string]idlTypeDef {
	m := make(map[string]idlTypeDef, len(idl.Types))
	for _, t := range idl.Types {
		m[t.Name] = t
	}
	return m
}

// decodeBorsh decodes borsh-encoded bytes into a map using the given field schema.
// types is the IDL type map (for defined/nested struct resolution); may be nil.
func decodeBorsh(data []byte, fields []idlField, types map[string]idlTypeDef) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(fields))
	offset := 0
	for _, field := range fields {
		val, n, err := decodeField(data[offset:], field.Type, types)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		result[field.Name] = val
		offset += n
	}
	return result, nil
}

func decodeField(data []byte, typ interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	switch t := typ.(type) {
	case string:
		return decodePrimitive(data, t, types)
	case map[string]interface{}:
		if inner, ok := t["vec"]; ok {
			return decodeVec(data, inner, types)
		}
		if inner, ok := t["option"]; ok {
			return decodeOption(data, inner, types)
		}
		if name, ok := t["defined"].(string); ok {
			return decodeDefined(data, name, types)
		}
		if inner, ok := t["array"]; ok {
			return decodeArray(data, inner, types)
		}
		return nil, 0, fmt.Errorf("unknown complex type descriptor: %v", t)
	default:
		return nil, 0, fmt.Errorf("unexpected type descriptor: %T %v", typ, typ)
	}
}

func decodePrimitive(data []byte, typ string, types map[string]idlTypeDef) (interface{}, int, error) {
	switch typ {
	case "bool":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for bool")
		}
		return data[0] != 0, 1, nil
	case "u8":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for u8")
		}
		return uint64(data[0]), 1, nil
	case "u16":
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for u16")
		}
		return uint64(binary.LittleEndian.Uint16(data[:2])), 2, nil
	case "u32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for u32")
		}
		return uint64(binary.LittleEndian.Uint32(data[:4])), 4, nil
	case "u64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for u64")
		}
		return binary.LittleEndian.Uint64(data[:8]), 8, nil
	case "u128":
		if len(data) < 16 {
			return nil, 0, fmt.Errorf("insufficient data for u128")
		}
		lo := binary.LittleEndian.Uint64(data[:8])
		hi := binary.LittleEndian.Uint64(data[8:16])
		val := new(big.Int).SetUint64(hi)
		val.Lsh(val, 64)
		val.Or(val, new(big.Int).SetUint64(lo))
		return val.String(), 16, nil
	case "i8":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for i8")
		}
		return int64(int8(data[0])), 1, nil
	case "i16":
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for i16")
		}
		return int64(int16(binary.LittleEndian.Uint16(data[:2]))), 2, nil
	case "i32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for i32")
		}
		return int64(int32(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case "i64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for i64")
		}
		return int64(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case "i128":
		if len(data) < 16 {
			return nil, 0, fmt.Errorf("insufficient data for i128")
		}
		lo := binary.LittleEndian.Uint64(data[:8])
		hi := binary.LittleEndian.Uint64(data[8:16])
		val := new(big.Int).SetUint64(hi)
		val.Lsh(val, 64)
		val.Or(val, new(big.Int).SetUint64(lo))
		if data[15]&0x80 != 0 {
			max := new(big.Int).Lsh(big.NewInt(1), 128)
			val.Sub(val, max)
		}
		return val.String(), 16, nil
	case "f32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for f32")
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case "f64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for f64")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case "publicKey":
		if len(data) < 32 {
			return nil, 0, fmt.Errorf("insufficient data for publicKey")
		}
		var pk solanago.PublicKey
		copy(pk[:], data[:32])
		return pk.String(), 32, nil
	case "string":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for string bytes (need %d, have %d)", length, len(data)-4)
		}
		return string(data[4 : 4+length]), 4 + length, nil
	case "bytes":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for bytes length")
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for bytes")
		}
		return hex.EncodeToString(data[4 : 4+length]), 4 + length, nil
	default:
		// Try as a defined type name
		if types != nil {
			return decodeDefined(data, typ, types)
		}
		return nil, 0, fmt.Errorf("unsupported primitive type: %q", typ)
	}
}

func decodeVec(data []byte, elemType interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("insufficient data for vec length")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	offset := 4
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		val, n, err := decodeField(data[offset:], elemType, types)
		if err != nil {
			return nil, 0, fmt.Errorf("vec[%d]: %w", i, err)
		}
		result = append(result, val)
		offset += n
	}
	return result, offset, nil
}

func decodeOption(data []byte, innerType interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("insufficient data for option discriminant")
	}
	if data[0] == 0 {
		return nil, 1, nil // None
	}
	val, n, err := decodeField(data[1:], innerType, types)
	if err != nil {
		return nil, 0, fmt.Errorf("option Some: %w", err)
	}
	return val, 1 + n, nil
}

func decodeDefined(data []byte, name string, types map[string]idlTypeDef) (interface{}, int, error) {
	if types == nil {
		return nil, 0, fmt.Errorf("no IDL types available to decode defined type %q", name)
	}
	def, ok := types[name]
	if !ok {
		return nil, 0, fmt.Errorf("unknown defined type %q", name)
	}
	result, err := decodeBorsh(data, def.Type.Fields, types)
	if err != nil {
		return nil, 0, fmt.Errorf("defined type %q: %w", name, err)
	}
	// Calculate total bytes consumed
	total := 0
	for _, f := range def.Type.Fields {
		_, n, err := decodeField(data[total:], f.Type, types)
		if err != nil {
			break
		}
		total += n
	}
	return result, total, nil
}

// decodeArray handles fixed-size arrays: {"array": ["u8", 32]}
func decodeArray(data []byte, spec interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	arr, ok := spec.([]interface{})
	if !ok || len(arr) != 2 {
		return nil, 0, fmt.Errorf("array spec must be [type, length], got %v", spec)
	}
	length, ok := arr[1].(float64) // JSON numbers decode as float64
	if !ok {
		return nil, 0, fmt.Errorf("array length must be a number, got %T", arr[1])
	}
	count := int(length)
	offset := 0
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		val, n, err := decodeField(data[offset:], arr[0], types)
		if err != nil {
			return nil, 0, fmt.Errorf("array[%d]: %w", i, err)
		}
		result = append(result, val)
		offset += n
	}
	return result, offset, nil
}
```

- [ ] **Step 4: Run borsh tests — expect pass**

```bash
go test ./internal/chains/solana/... -run TestDecodeBorsh -v
```

Expected: all borsh tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/borsh.go internal/chains/solana/borsh_test.go
git commit -m "feat(solana): add dynamic borsh decoder for Anchor IDL events"
```

---

## Task 8: Parser

**Files:**
- Create: `internal/chains/solana/parser.go`
- Create: `internal/chains/solana/parser_test.go`

- [ ] **Step 1: Write failing parser tests**

Create `internal/chains/solana/parser_test.go`:

```go
package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// buildTransferEvent creates borsh bytes for a Transfer event: from(publicKey), amount(u64)
func buildTransferEvent(amount uint64) (discriminator []byte, payload []byte) {
	h := sha256.Sum256([]byte("event:Transfer"))
	disc := h[:8]
	buf := make([]byte, 32+8)                         // publicKey(32) + u64(8)
	binary.LittleEndian.PutUint64(buf[32:], amount)   // amount
	return disc, buf
}

const transferIDL = `{
  "events": [
    {
      "name": "Transfer",
      "fields": [
        {"name": "from", "type": "publicKey"},
        {"name": "amount", "type": "u64"}
      ]
    }
  ],
  "types": []
}`

func TestParser_IDLPath(t *testing.T) {
	disc, payload := buildTransferEvent(9999)
	rawData := append(disc, payload...)

	raw := core.RawEvent{
		Address:  "TokenkegQ",
		EventKey: "TokenkegQ:" + strings.Repeat("00", 8),
		RawData:  rawData,
		Metadata: map[string]string{"idl": transferIDL},
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["amount"] != uint64(9999) {
		t.Errorf("expected amount=9999, got %v", result["amount"])
	}
	if result["_event_name"] != "Transfer" {
		t.Errorf("expected _event_name=Transfer, got %v", result["_event_name"])
	}
}

func TestParser_RawFallback_UnknownDiscriminator(t *testing.T) {
	rawData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xAA, 0xBB}
	raw := core.RawEvent{
		Address:  "SomeProg",
		EventKey: "SomeProg:0102030405060708",
		RawData:  rawData,
		Metadata: map[string]string{"idl": transferIDL}, // IDL exists but discriminator doesn't match
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected raw fallback result")
	}
	if result["discriminator"] != "0102030405060708" {
		t.Errorf("expected discriminator in fallback, got %v", result["discriminator"])
	}
	if _, ok := result["data_base64"]; !ok {
		t.Error("expected data_base64 in fallback")
	}
}

func TestParser_RawFallback_NoIDL(t *testing.T) {
	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03, 0xFF}
	raw := core.RawEvent{
		Address:  "SomeProg",
		EventKey: "SomeProg:deadbeef00010203",
		RawData:  rawData,
		Metadata: map[string]string{}, // no IDL
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["discriminator"] != "deadbeef00010203" {
		t.Errorf("expected discriminator, got %v", result["discriminator"])
	}
}

func TestParser_EventKey_PassThrough(t *testing.T) {
	p := NewEventParser()
	key := "TokenkegQ:aabbccdd00112233"
	if got := p.EventKey(key); got != key {
		t.Errorf("EventKey should pass through: expected %q, got %q", key, got)
	}
}

func TestParser_IDL_Discriminator(t *testing.T) {
	// Verify that discriminator computation is SHA256("event:Transfer")[0:8]
	h := sha256.Sum256([]byte("event:Transfer"))
	expected := h[:8]

	var idl anchorIDL
	json.Unmarshal([]byte(transferIDL), &idl)  //nolint:errcheck

	p := NewEventParser()
	disc := p.computeDiscriminator("Transfer")
	for i, b := range expected {
		if disc[i] != b {
			t.Errorf("discriminator byte %d: expected %02x, got %02x", i, b, disc[i])
		}
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestParser -v 2>&1 | head -10
```

Expected: compile error (`NewEventParser` not defined).

- [ ] **Step 3: Implement `parser.go`**

Create `internal/chains/solana/parser.go`:

```go
package solana

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// EventParser implements core.EventParser for Solana.
// It decodes Anchor IDL events via discriminator + borsh, with raw fallback.
type EventParser struct{}

// NewEventParser creates an EventParser.
func NewEventParser() *EventParser { return &EventParser{} }

// EventKey returns the identifier as-is.
// For Solana, EventKey is already canonical: "programAddress:discriminatorHex".
func (p *EventParser) EventKey(identifier string) string { return identifier }

// ParseEvent decodes a raw program event.
// If an IDL is available and the discriminator matches, uses borsh decoding.
// Otherwise falls back to returning the raw bytes as hex/base64.
func (p *EventParser) ParseEvent(raw core.RawEvent) (map[string]interface{}, error) {
	if len(raw.RawData) < 8 {
		return nil, fmt.Errorf("solana parser: event data too short (%d bytes)", len(raw.RawData))
	}

	discriminator := raw.RawData[:8]
	payload := raw.RawData[8:]

	// Attempt IDL-based decoding if IDL is present
	idlJSON := raw.Metadata["idl"]
	if idlJSON != "" {
		result, err := p.decodeWithIDL(discriminator, payload, idlJSON)
		if err == nil && result != nil {
			return result, nil
		}
		// IDL decode failed (unknown discriminator or borsh error) → fall through to raw
	}

	return p.rawFallback(discriminator, raw.RawData), nil
}

// decodeWithIDL tries to find the event matching discriminator in the IDL and borsh-decode it.
// Returns nil, nil if the discriminator does not match any event.
func (p *EventParser) decodeWithIDL(discriminator, payload []byte, idlJSON string) (map[string]interface{}, error) {
	idl, err := parseIDL(idlJSON)
	if err != nil {
		return nil, err
	}
	typeMap := buildTypeMap(idl)

	for _, event := range idl.Events {
		expected := p.computeDiscriminator(event.Name)
		if !bytesEqual(discriminator, expected[:]) {
			continue
		}
		// Found matching event
		fields, err := decodeBorsh(payload, event.Fields, typeMap)
		if err != nil {
			return nil, fmt.Errorf("borsh decode for event %q: %w", event.Name, err)
		}
		fields["_event_name"] = event.Name
		return fields, nil
	}

	return nil, nil // no matching event
}

// computeDiscriminator returns sha256("event:<name>")[0:8].
func (p *EventParser) computeDiscriminator(eventName string) [8]byte {
	h := sha256.Sum256([]byte("event:" + eventName))
	var disc [8]byte
	copy(disc[:], h[:8])
	return disc
}

// rawFallback returns discriminator + base64 + hex for unknown events.
func (p *EventParser) rawFallback(discriminator, fullData []byte) map[string]interface{} {
	return map[string]interface{}{
		"discriminator": hex.EncodeToString(discriminator),
		"data_base64":   base64.StdEncoding.EncodeToString(fullData),
		"data_hex":      hex.EncodeToString(fullData),
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Run all parser tests — expect pass**

```bash
go test ./internal/chains/solana/... -run TestParser -v
```

Expected: all parser tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/parser.go internal/chains/solana/parser_test.go
git commit -m "feat(solana): add EventParser with IDL discriminator + borsh + raw fallback"
```

---

## Task 9: Storage

**Files:**
- Create: `internal/chains/solana/storage.go`
- Create: `internal/chains/solana/storage_test.go`

- [ ] **Step 1: Write failing storage tests**

Create `internal/chains/solana/storage_test.go`:

```go
package solana

import (
	"context"
	"testing"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
				"network_id":  networkID.Hex(),
				"program_id":  programID.Hex(),
				"block_hash":  "bhash1",
				"slot":        "500",
				"log_index":   "2",
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
		BlockNumber: 600, TransactionHash: "sig2", Address: "Prog1",
		EventKey: "Prog1:0102030405060708",
		RawData:  []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		Metadata: map[string]string{
			"network_id": networkID.Hex(), "program_id": programID.Hex(),
			"slot": "600", "log_index": "0", "block_hash": "bh", "account_keys": "[]",
		},
	}

	// Insert twice — second should be ignored (unique index)
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
		NetworkID: networkID, Address: "ProgA",
		Status: models.StatusActive, StartSlot: 100,
	}
	_, err := s.Programs().InsertOne(context.Background(), prog)
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	if err := s.UpdateScanProgress(context.Background(), "ProgA", 999); err != nil {
		t.Fatalf("UpdateScanProgress: %v", err)
	}

	var updated Program
	s.Programs().FindOne(context.Background(), bson.M{"address": "ProgA"}).Decode(&updated) //nolint:errcheck
	if updated.LastScannedSlot != 999 {
		t.Errorf("expected last_scanned_slot=999, got %d", updated.LastScannedSlot)
	}
}

func TestStorage_LoadContracts(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	networkID := primitive.NewObjectID()
	progs := []interface{}{
		Program{NetworkID: networkID, Address: "ProgA", Events: []string{"Transfer"},
			StartSlot: 100, LastScannedSlot: 200, Status: models.StatusActive},
		Program{NetworkID: networkID, Address: "ProgB", Events: []string{"Mint"},
			StartSlot: 50, Status: models.StatusActive},
		Program{NetworkID: networkID, Address: "ProgC", Status: "deactive"}, // should be excluded
	}
	s.Programs().InsertMany(context.Background(), progs) //nolint:errcheck

	contracts, err := s.LoadContracts(context.Background())
	if err != nil {
		t.Fatalf("LoadContracts: %v", err)
	}
	if len(contracts) != 2 {
		t.Fatalf("expected 2 active programs, got %d", len(contracts))
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/chains/solana/... -run TestStorage 2>&1 | head -10
```

Expected: compile error (`NewStorage` not defined).

- [ ] **Step 3: Implement `storage.go`**

Create `internal/chains/solana/storage.go`:

```go
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
		json.Unmarshal([]byte(ev.Metadata["account_keys"]), &accountKeys) //nolint:errcheck

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
	// Networks
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := s.Networks().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	// Programs
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "address", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if _, err := s.Programs().Indexes().CreateOne(ctx, model); err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	// ProgramLogs
	for _, model := range []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "network_id", Value: 1}, {Key: "program_id", Value: 1},
				{Key: "slot", Value: 1}, {Key: "transaction_hash", Value: 1}, {Key: "log_index", Value: 1},
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
	// DecodedEvents
	for _, model := range []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "program_address", Value: 1}, {Key: "event_name", Value: 1},
				{Key: "slot", Value: 1}, {Key: "transaction_hash", Value: 1}, {Key: "log_index", Value: 1},
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
```

- [ ] **Step 4: Run storage tests (requires MongoDB)**

```bash
go test ./internal/chains/solana/... -run TestStorage -v
```

If MongoDB is running locally: Expected: all tests PASS.
If not available: tests will be skipped (the `t.Skipf` guard in `setupTestStorage`).

- [ ] **Step 5: Commit**

```bash
git add internal/chains/solana/storage.go internal/chains/solana/storage_test.go
git commit -m "feat(solana): add Storage implementing core.StorageInterface"
```

---

## Task 10: Handler

**Files:**
- Create: `internal/chains/solana/handler.go`

- [ ] **Step 1: Create `handler.go`**

Create `internal/chains/solana/handler.go`:

```go
package solana

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// decodedEventDoc is the document shape written to decoded_events.
type decodedEventDoc struct {
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ProgramAddress  string                 `bson:"program_address"`
	EventName       string                 `bson:"event_name"`
	Slot            uint64                 `bson:"slot"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        int                    `bson:"log_index"`
	Data            map[string]interface{} `bson:"data"`
	CreatedAt       time.Time              `bson:"created_at"`
}

// DefaultHandler returns a core.Handler that:
//  1. Saves raw events to program_logs
//  2. Decodes each event via the EventParser
//  3. Saves decoded events to decoded_events
func DefaultHandler(store *Storage, parser *EventParser) core.Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		// 1. Save raw logs
		if err := store.SaveEvents(ctx, batch); err != nil {
			return err
		}

		// 2. Decode and save
		now := time.Now()
		var decoded []interface{}
		for _, ev := range batch {
			fields, err := parser.ParseEvent(ev)
			if err != nil {
				log.Printf("solana handler: parse event: %v", err)
				continue
			}
			if fields == nil {
				continue
			}

			eventName, _ := fields["_event_name"].(string)
			if eventName == "" {
				// Raw fallback — use discriminator as name
				eventName, _ = fields["discriminator"].(string)
			}

			networkID, _ := primitive.ObjectIDFromHex(ev.Metadata["network_id"])
			logIndex, _ := strconv.Atoi(ev.Metadata["log_index"])

			// Remove internal _event_name from the stored data map
			dataCopy := make(map[string]interface{}, len(fields))
			for k, v := range fields {
				if k != "_event_name" {
					dataCopy[k] = v
				}
			}

			decoded = append(decoded, decodedEventDoc{
				NetworkID:       networkID,
				ProgramAddress:  ev.Address,
				EventName:       eventName,
				Slot:            ev.BlockNumber,
				TransactionHash: ev.TransactionHash,
				LogIndex:        logIndex,
				Data:            dataCopy,
				CreatedAt:       now,
			})
		}

		if len(decoded) > 0 {
			if err := store.SaveDecodedEvents(ctx, decoded); err != nil {
				log.Printf("solana handler: save decoded events: %v", err)
				// Don't return error — raw logs are already saved
			}
		}

		return nil
	}
}
```

- [ ] **Step 2: Build to verify**

```bash
go build ./internal/chains/solana/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/chains/solana/handler.go
git commit -m "feat(solana): add DefaultHandler for raw + decoded event storage"
```

---

## Task 11: Wire Up main.go + Seed File

**Files:**
- Modify: `cmd/indexer/main.go`
- Create: `cmd/seed/data/solana-devnet.json`

- [ ] **Step 1: Add `runSolana` to `cmd/indexer/main.go`**

Open `cmd/indexer/main.go` and make these two changes:

**Change 1** — add import for the solana package. In the existing import block, add:
```go
solanachain "github.com/1hoodlab/go-indexing.git/internal/chains/solana"
```

**Change 2** — in the `switch cfg.ChainType` block (currently line 21), change:
```go
	switch cfg.ChainType {
	case "evm":
		runEVM(cfg)
	default:
		log.Fatalf("unsupported chain type: %s", cfg.ChainType)
	}
```
to:
```go
	switch cfg.ChainType {
	case "evm":
		runEVM(cfg)
	case "solana":
		runSolana(cfg)
	default:
		log.Fatalf("unsupported chain type: %s", cfg.ChainType)
	}
```

**Change 3** — add `runSolana` function at the end of the file:
```go
func runSolana(cfg config.Config) {
	store, err := solanachain.NewStorage(cfg.MongoURI, cfg.Database)
	if err != nil {
		log.Fatalf("solana: failed to connect to MongoDB: %v", err)
	}
	defer store.Close(context.Background())

	if err := store.EnsureIndexes(context.Background()); err != nil {
		log.Fatalf("solana: failed to ensure indexes: %v", err)
	}

	contracts, err := store.LoadContracts(context.Background())
	if err != nil {
		log.Fatalf("solana: failed to load contracts: %v", err)
	}
	if len(contracts) == 0 {
		log.Fatal("solana: no active programs to index")
	}

	programModels, err := store.LoadProgramModels(context.Background())
	if err != nil {
		log.Fatalf("solana: failed to load program models: %v", err)
	}

	network, err := store.LoadNetwork(context.Background())
	if err != nil {
		log.Fatalf("solana: failed to load network: %v", err)
	}
	if len(network.RPCURLs) == 0 {
		log.Fatal("solana: network has no RPC URLs configured in database")
	}

	pool, err := solanachain.NewRPCPool(network.RPCURLs)
	if err != nil {
		log.Fatalf("solana: failed to create RPC pool: %v", err)
	}

	networkID := network.ID.Hex()
	programMap := make(map[string]solanachain.ProgramInfoExport, len(programModels))
	for _, prog := range programModels {
		programMap[prog.Address] = solanachain.ProgramInfoExport{ID: prog.ID.Hex(), IDL: prog.IDL}
	}

	subscriber := solanachain.NewSubscriber(pool, cfg.PollInterval)
	fetcher := solanachain.NewLogFetcherFromExport(pool, networkID, programMap)
	parser := solanachain.NewEventParser()
	handler := solanachain.DefaultHandler(store, parser)

	registry := core.NewRegistry(handler)

	p := pipeline.New(cfg.ChainName, subscriber, fetcher, registry, store, contracts)
	if err := p.Start(); err != nil {
		log.Fatalf("solana: failed to start pipeline: %v", err)
	}
	defer p.Stop()

	log.Printf("[%s] solana indexer started (rpc=%s)", cfg.ChainName, pool.ActiveURL())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")
}
```

- [ ] **Step 2: Add exported helper types/constructors to `fetcher.go`**

The `runSolana` function above uses `ProgramInfoExport` and `NewLogFetcherFromExport`. Add these to `internal/chains/solana/fetcher.go`:

```go
// ProgramInfoExport is the exported version of programInfo for use in main.go.
type ProgramInfoExport struct {
	ID  string
	IDL string
}

// NewLogFetcherFromExport creates a LogFetcher from the exported ProgramInfoExport map.
func NewLogFetcherFromExport(client SolanaRPCClient, networkID string, programs map[string]ProgramInfoExport) *LogFetcher {
	pm := make(map[string]programInfo, len(programs))
	for addr, p := range programs {
		pm[addr] = programInfo{id: p.ID, idl: p.IDL}
	}
	return NewLogFetcher(client, networkID, pm)
}
```

- [ ] **Step 3: Build to verify all wiring**

```bash
go build ./cmd/indexer/...
```

Expected: no errors.

- [ ] **Step 4: Create seed file `cmd/seed/data/solana-devnet.json`**

Create `cmd/seed/data/solana-devnet.json`:

```json
{
  "network": {
    "name": "Solana Devnet",
    "chain_type": "solana",
    "chain_id": 0,
    "rpc_urls": ["https://api.devnet.solana.com"],
    "type": "testnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "11111111111111111111111111111111",
      "events": [],
      "idl": "",
      "start_block": 1
    }
  ]
}
```

- [ ] **Step 5: Run full test suite**

```bash
go test ./... 2>&1 | tail -20
```

Expected: all existing tests still pass; new Solana tests pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/indexer/main.go internal/chains/solana/fetcher.go cmd/seed/data/solana-devnet.json
git commit -m "feat(solana): wire up runSolana in main.go and add devnet seed file"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| HTTP polling getSlot + getBlocksWithLimit | Task 4 (Subscriber) |
| 400ms poll interval | Task 4 (configurable via PollInterval) |
| `confirmed` commitment | Task 3 (baked into Client.GetSlot, GetBlock, GetBlocksWithLimit) |
| logMessages scope tracking | Task 5 (extractProgramEvents) |
| Anchor IDL discriminator | Task 8 (EventParser.computeDiscriminator) |
| Borsh decoding | Task 7 (borsh.go) |
| Raw fallback | Task 8 (EventParser.rawFallback) |
| EventKey = programAddress:discriminatorHex | Task 6 (LogFetcher.fetchLogsFromBlock) |
| gagliardetto/solana-go | Task 1 + Task 3 |
| program_logs collection | Task 9 (Storage.SaveEvents) |
| decoded_events collection | Task 9 (Storage.SaveDecodedEvents) |
| programs collection | Task 9 (Storage.LoadContracts, UpdateScanProgress) |
| Unique indexes (idempotency) | Task 9 (Storage.EnsureIndexes) |
| Versioned tx loadedAddresses merge | Task 3 (Client.GetBlock) |
| RPC pool fallback | Task 3 (rpcpool.go) |
| main.go routing | Task 11 |
| Seed file | Task 11 |

**No gaps found.**
