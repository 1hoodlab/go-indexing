# PostgreSQL Indexer, Queue & Webhook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the MongoDB-backed EVM indexer path with PostgreSQL: `networks` (multi-RPC + fallback), `contracts` + `evm_event_signatures`, checkpoint-driven scanning, `blocks` + `eth_logs`, and transactional `outbox` with a webhook consumer.

**Architecture:** One indexer process per network loads `networks.rpc_urls` into the existing `evm.RPCPool`, loads active `contracts` joined with `evm_event_signatures`, polls blocks from `indexer_config.checkpoint`, and uses a **dual log-fetch strategy** (mirroring `indexer-service`): `eth_getLogs` for catch-up batches, `eth_getBlockReceipts` for genesis + tip-following. Logs are filtered in-memory by `topic0`, then confirmed blocks/logs/outbox rows are persisted in a single PostgreSQL transaction. Solana stays on MongoDB until a follow-up plan.

**Tech Stack:** Go 1.25, `github.com/jackc/pgx/v5`, `github.com/pressly/goose/v3`, existing `internal/chains/evm/rpcpool.go`, PostgreSQL 16, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-07-24-postgres-indexer-queue-design.md`

## Global Constraints

- Database for EVM indexer path: **PostgreSQL** (MongoDB path remains for Solana only in this plan).
- Queue: in-DB **`outbox`** table (transactional outbox); no Redis/RabbitMQ.
- Event listen config: **`evm_event_signatures`** table; **no** `events[]` on `contracts`.
- RPC source: **`networks.rpc_urls`** JSONB array; **not** env `RPC_URL`.
- RPC fallback: reuse **`evm.NewRPCPool`** — first URL primary, rotate on error, failed URLs removed until restart.
- Log fetch (dual strategy, same as `indexer-service`):
  - **Catch-up** (`head - confirmations > checkpoint`): `eth_getLogs` with `address` filter + batch up to `block_batch_size`.
  - **Genesis** (`checkpoint == 0`) and **tip-following** (`checkpoint` near head): `eth_getBlockReceipts(blockHash)` → flatten `receipt.logs` → filter in-memory by `address` + `topic0`.
  - **Why receipts at tip:** one RPC call per block; receipts available before node log index catches up; enables future factory re-scan on in-memory logs.
- Scan anchor: **`indexer_config.checkpoint`** only — no `start_block` on contracts.
- Outbox enqueue: only when block reaches **`confirmation_depth`**.
- Webhook idempotency: dedup on `eth_logs`; skip `outbox` insert if `log_id` already exists.
- Numeric decoded values: store as **strings** in `decoded_params` JSONB.
- Multi-chain deploy: **one PostgreSQL database per chain instance** (same as current one-process-per-chain model).

---

## Log Fetch Strategy (reference: `indexer-service`)

```text
                    ┌─────────────────────────────────────┐
                    │           run() loop                │
                    └─────────────────────────────────────┘
                                      │
          checkpoint == 0 ────────────┼──────────── else
                  │                 │                 │
                  ▼                 │                 ▼
         getBlock('latest')         │     head - confirmations > checkpoint?
         getBlockReceipts           │           │              │
         filter in-memory           │          yes             no
                  │                 │           │              │
                  │                 │           ▼              ▼
                  │                 │    eth_getLogs      tip_following?
                  │                 │    (address filter)      │      │
                  │                 │    batch range        yes     no
                  │                 │           │              │      sleep
                  │                 │           │              ▼
                  │                 │           │     getBlock(N+1)
                  │                 │           │     getBlockReceipts
                  │                 │           │     filter in-memory
                  └─────────────────┴───────────┴──────────────┘
                                      │
                              processLogs (topic0 filter)
```

| Mode | RPC | Address filter | When |
|------|-----|----------------|------|
| Catch-up | `eth_getLogs` | RPC-side (`addresses`) | Historical gap, `latest - confirmations > checkpoint` |
| Tip-following | `eth_getBlockReceipts` | In-memory | Near head, one block at a time |
| Genesis | `eth_getBlockReceipts` | In-memory | `checkpoint == 0`, bootstrap from latest |

`getBlockReceipts` retry: if RPC returns `null` (block/receipts not ready), retry up to 5× with `retry_after_receipts_null` sleep — same as `indexer-service`.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-compose.yml` | Modify | Add `postgres` service alongside MongoDB |
| `.env.example` | Modify | Add `POSTGRES_DSN`, `STORAGE_BACKEND` |
| `internal/config/config.go` | Modify | Postgres DSN, storage backend switch |
| `migrations/*.sql` | Create | Goose SQL migrations (all tables + enums) |
| `internal/storage/postgres/postgres.go` | Create | `pgxpool` connect, health, close |
| `internal/storage/postgres/network.go` | Create | Load active network + `rpc_urls` |
| `internal/storage/postgres/contract.go` | Create | Contract upsert + load |
| `internal/storage/postgres/evm_event_signature.go` | Create | Bulk insert + load active signatures |
| `internal/storage/postgres/indexer_config.go` | Create | Get/set checkpoint, scan_enabled, confirmation_depth |
| `internal/storage/postgres/indexer_config_test.go` | Create | Config read/write tests |
| `internal/storage/postgres/evm_event_signature_test.go` | Create | Unique (contract_id, topic0) tests |
| `internal/chains/evm/indexer_config.go` | Create | `IndexerRuntimeConfig` + join query |
| `internal/chains/evm/client.go` | Modify | Add `GetBlockReceipts`, `FilterLogs` range helper |
| `internal/chains/evm/receipts.go` | Create | `RpcReceipt` types, flatten logs, retry wrapper |
| `internal/chains/evm/receipts_test.go` | Create | Flatten + retry tests |
| `internal/chains/evm/rpcpool.go` | Modify | Extend `RPCClient` with `GetBlockReceipts`, `FilterLogs` range |
| `internal/chains/evm/log_processor.go` | Create | Shared `processLogs` — topic0 filter + decode (used by both fetch paths) |
| `internal/chains/evm/fetcher.go` | Modify | Catch-up path via `eth_getLogs`; delegate filter to `log_processor` |
| `internal/chains/evm/fetcher_test.go` | Create | Topic0 filter + getLogs path tests |
| `internal/chains/evm/block_fetcher.go` | Create | Tip/genesis path via `getBlockReceipts` per block |
| `internal/chains/evm/block_fetcher_test.go` | Create | Receipt flatten + in-memory filter tests |
| `internal/chains/evm/block_indexer.go` | Create | Checkpoint loop, confirmation depth, TX insert |
| `internal/chains/evm/block_indexer_test.go` | Create | Transaction + dedup tests (testcontainers or sqlmock) |
| `internal/chains/evm/decode.go` | Create | ABI decode → `decoded_params` map |
| `cmd/seed-pg/main.go` | Create | Seed networks, contracts, evm_event_signatures, indexer_config |
| `cmd/migrate/main.go` | Create | Run goose migrations up/down |
| `cmd/consumer/main.go` | Create | Outbox poller → webhook POST |
| `cmd/indexer/main.go` | Modify | `STORAGE_BACKEND=postgres` EVM path |
| `pkg/utils/signature.go` | Exists | `EventTopic0`, used by seed + fetcher |
| `README.md` | Modify | Postgres setup, seed, run indexer + consumer |

---

## Phase Overview

| Phase | Tasks | Deliverable |
|-------|-------|-------------|
| **A — Infra** | 1–3 | Postgres running, migrations applied, config wired |
| **B — Config layer** | 4–6 | Seed + load network RPCs, contracts, event signatures |
| **C — Indexer core** | 7–11 | `getBlockReceipts` + `getLogs`, topic0 filter, block scan, TX persist |
| **D — Consumer** | 12 | Outbox → webhook delivery |
| **E — Integration** | 13 | End-to-end manual test on Base/Sepolia |

---

## Task 1: Postgres Infrastructure

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Modify: `internal/config/config.go`

**Interfaces:**
- Produces: `config.Config.PostgresDSN string`, `config.Config.StorageBackend string` (`"mongo"` | `"postgres"`)

- [ ] **Step 1: Add postgres service to docker-compose**

```yaml
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: indexer
      POSTGRES_PASSWORD: indexer
      POSTGRES_DB: indexing
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U indexer -d indexing"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  mongodb_data:
  postgres_data:
```

- [ ] **Step 2: Extend config**

Add to `internal/config/config.go`:

```go
type Config struct {
	ChainType       string
	ChainName       string
	PollInterval    time.Duration
	MongoURI        string
	Database        string
	PostgresDSN     string
	StorageBackend  string // "mongo" | "postgres"
}

// In Load():
PostgresDSN:    getenv("POSTGRES_DSN", "postgres://indexer:indexer@localhost:5432/indexing?sslmode=disable"),
StorageBackend: getenv("STORAGE_BACKEND", "mongo"),
```

Add to `.env.example`:

```env
STORAGE_BACKEND=mongo
POSTGRES_DSN=postgres://indexer:indexer@localhost:5432/indexing?sslmode=disable
```

- [ ] **Step 3: Verify postgres starts**

```bash
docker compose up -d postgres
docker compose exec postgres pg_isready -U indexer -d indexing
```

Expected: `accepting connections`

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example internal/config/config.go
git commit -m "feat(infra): add PostgreSQL service and config"
```

---

## Task 2: Dependencies & Migration Runner

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `cmd/migrate/main.go`

**Interfaces:**
- Produces: `go run ./cmd/migrate -- up` applies all SQL in `migrations/`

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/jackc/pgx/v5/pgxpool
go get github.com/pressly/goose/v3
go mod tidy
```

- [ ] **Step 2: Create migrate command**

Create `cmd/migrate/main.go`:

```go
package main

import (
	"database/sql"
	"flag"
	"log"
	"os"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	dir := flag.String("dir", "migrations", "migrations directory")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("usage: go run ./cmd/migrate -- up|down|status")
	}

	cfg := config.Load()
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatalf("dialect: %v", err)
	}
	if err := goose.Run(args[0], db, *dir, args[1:]...); err != nil {
		log.Fatalf("goose %s: %v", args[0], err)
	}
	os.Exit(0)
}
```

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum cmd/migrate/main.go
git commit -m "feat(migrate): add goose migration runner"
```

---

## Task 3: SQL Migrations

**Files:**
- Create: `migrations/00001_init_enums.sql`
- Create: `migrations/00002_networks.sql`
- Create: `migrations/00003_indexer_config.sql`
- Create: `migrations/00004_contracts.sql`
- Create: `migrations/00005_evm_event_signatures.sql`
- Create: `migrations/00006_blocks.sql`
- Create: `migrations/00007_eth_logs.sql`
- Create: `migrations/00008_outbox.sql`
- Create: `migrations/00009_webhook.sql`

Copy SQL verbatim from spec sections **Enums**, **1. networks**, **2. indexer_config**, **3. contracts**, **4. evm_event_signatures**, **5. blocks**, **6. eth_logs**, **7. outbox**, **8–9. webhook_***.

`migrations/00005_evm_event_signatures.sql` (core of this plan):

```sql
-- +goose Up
CREATE TABLE evm_event_signatures (
    id           BIGSERIAL PRIMARY KEY,
    contract_id  BIGINT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    event_name   TEXT NOT NULL,
    signature    TEXT NOT NULL,
    topic0       TEXT NOT NULL,
    status       contract_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT evm_event_signatures_contract_topic0_unique UNIQUE (contract_id, topic0),
    CONSTRAINT evm_event_signatures_topic0_format CHECK (topic0 ~ '^0x[0-9a-f]{64}$')
);
CREATE INDEX idx_evm_event_signatures_contract ON evm_event_signatures (contract_id);
CREATE INDEX idx_evm_event_signatures_topic0 ON evm_event_signatures (topic0);
CREATE INDEX idx_evm_event_signatures_active
    ON evm_event_signatures (contract_id, status) WHERE status = 'active';

-- +goose Down
DROP TABLE IF EXISTS evm_event_signatures;
```

- [ ] **Step 1: Create all migration files** (copy from spec)

- [ ] **Step 2: Run migrations**

```bash
go run ./cmd/migrate -- up
go run ./cmd/migrate -- status
```

Expected: all migrations `Applied`

- [ ] **Step 3: Commit**

```bash
git add migrations/
git commit -m "feat(db): add PostgreSQL schema migrations"
```

---

## Task 4: Postgres Storage — Network & RPC URLs

**Files:**
- Create: `internal/storage/postgres/postgres.go`
- Create: `internal/storage/postgres/network.go`
- Create: `internal/storage/postgres/network_test.go`

**Interfaces:**
- Produces: `type Network struct { ID int64; Name string; ChainID int64; ChainType string; RPCURLs []string; Status string }`
- Produces: `func (s *Store) LoadActiveNetwork(ctx context.Context) (*Network, error)`

- [ ] **Step 1: Write failing test**

Create `internal/storage/postgres/network_test.go`:

```go
func TestLoadActiveNetwork_ReturnsRPCURLs(t *testing.T) {
	// requires test DB or skip if POSTGRES_DSN unset
	if os.Getenv("POSTGRES_DSN") == "" {
		t.Skip("POSTGRES_DSN not set")
	}
	ctx := context.Background()
	store, err := New(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, err = store.pool.Exec(ctx, `
		INSERT INTO networks (name, chain_id, chain_type, rpc_urls, status)
		VALUES ('Test', 8453, 'evm', '["https://rpc1.example","https://rpc2.example"]', 'active')
		ON CONFLICT (chain_id) DO UPDATE SET rpc_urls = EXCLUDED.rpc_urls`)
	if err != nil {
		t.Fatal(err)
	}

	net, err := store.LoadActiveNetwork(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(net.RPCURLs) != 2 {
		t.Fatalf("want 2 rpc urls, got %d", len(net.RPCURLs))
	}
}
```

- [ ] **Step 2: Implement postgres store**

Create `internal/storage/postgres/postgres.go`:

```go
package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
```

Create `internal/storage/postgres/network.go`:

```go
type Network struct {
	ID        int64
	Name      string
	ChainID   int64
	ChainType string
	RPCURLs   []string
	Status    string
}

func (s *Store) LoadActiveNetwork(ctx context.Context) (*Network, error) {
	var n Network
	var rpcJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, chain_id, chain_type, rpc_urls, status
		FROM networks WHERE status = 'active' ORDER BY id LIMIT 1`,
	).Scan(&n.ID, &n.Name, &n.ChainID, &n.ChainType, &rpcJSON, &n.Status)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rpcJSON, &n.RPCURLs); err != nil {
		return nil, err
	}
	return &n, nil
}
```

- [ ] **Step 3: Run test**

```bash
POSTGRES_DSN=postgres://indexer:indexer@localhost:5432/indexing?sslmode=disable go test ./internal/storage/postgres/... -run TestLoadActiveNetwork -v
```

- [ ] **Step 4: Commit**

```bash
git add internal/storage/postgres/
git commit -m "feat(postgres): load active network with rpc_urls"
```

---

## Task 5: Contracts & evm_event_signatures Repositories

**Files:**
- Create: `internal/storage/postgres/contract.go`
- Create: `internal/storage/postgres/evm_event_signature.go`
- Create: `internal/storage/postgres/evm_event_signature_test.go`

**Interfaces:**
- Produces: `func UpsertContract(ctx, networkID int64, address string, abi json.RawMessage) (contractID int64, error)`
- Produces: `func BulkInsertEventSignatures(ctx, contractID int64, sigs []EventSignatureInput) error`
- Produces: `type EventSignatureInput struct { EventName, Signature, Topic0 string }`

- [ ] **Step 1: Write failing test for idempotent bulk insert**

```go
func TestBulkInsertEventSignatures_Idempotent(t *testing.T) {
	// insert same (contract_id, topic0) twice → no error, still 1 row
}
```

- [ ] **Step 2: Implement bulk insert**

```go
func (s *Store) BulkInsertEventSignatures(ctx context.Context, contractID int64, rows []EventSignatureInput) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`
			INSERT INTO evm_event_signatures (contract_id, event_name, signature, topic0)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (contract_id, topic0) DO NOTHING`,
			contractID, r.EventName, r.Signature, r.Topic0)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 3: Add helper to parse event name from signature**

In `pkg/utils/signature.go` add:

```go
// EventNameFromSignature returns the name before '('.
// "Transfer(address,address,uint256)" → "Transfer"
func EventNameFromSignature(canonical string) string {
	if i := strings.Index(canonical, "("); i > 0 {
		return canonical[:i]
	}
	return canonical
}
```

- [ ] **Step 4: Run tests and commit**

```bash
go test ./internal/storage/postgres/... -v
git add internal/storage/postgres/ pkg/utils/signature.go
git commit -m "feat(postgres): contracts and evm_event_signatures repositories"
```

---

## Task 6: Postgres Seed Command

**Files:**
- Create: `cmd/seed-pg/main.go`

Reuses existing seed JSON format (`cmd/seed/data/*.json`): `network` + `contracts[].events[]`.

- [ ] **Step 1: Implement seed-pg**

```go
// For each seed file:
// 1. Upsert networks (chain_id unique) with rpc_urls from JSON
// 2. Upsert indexer_config keys: scan_enabled, checkpoint (from first contract start_block or env), confirmation_depth=12
// 3. For each contract: UpsertContract → for each events[] string:
//      topic0 := utils.EventTopic0(sig)
//      eventName := utils.EventNameFromSignature(sig)
//      BulkInsertEventSignatures
```

Checkpoint seed rule: use `min(contracts[].start_block)` or explicit `checkpoint` field if added to JSON later.

- [ ] **Step 2: Seed Base example**

```bash
go run ./cmd/seed-pg --config cmd/seed/data/base.json
```

Verify:

```sql
SELECT c.address, e.event_name, e.topic0, e.status
FROM contracts c
JOIN evm_event_signatures e ON e.contract_id = c.id;
```

- [ ] **Step 3: Commit**

```bash
git add cmd/seed-pg/
git commit -m "feat(seed): PostgreSQL seed for networks, contracts, event signatures"
```

---

## Task 7: Load Indexer Config (contracts + event signatures join)

**Files:**
- Create: `internal/chains/evm/indexer_config.go`
- Create: `internal/storage/postgres/indexer_config.go`

**Interfaces:**
- Produces:

```go
type ContractEvent struct {
	ContractID int64
	Address    string
	ABI        json.RawMessage
	EventName  string
	Signature  string
	Topic0     string
}

type IndexerRuntimeConfig struct {
	NetworkID          int64
	ChainID            int64
	RPCURLs            []string
	Checkpoint         uint64
	ScanEnabled        bool
	ConfirmationDepth  int
	ContractEvents     []ContractEvent
	AllowedTopic0ByAddr map[string]map[string]struct{} // built at load time
}

func (s *Store) LoadIndexerRuntimeConfig(ctx context.Context) (*IndexerRuntimeConfig, error)
```

- [ ] **Step 1: Write failing test** — join returns only `active` contracts + signatures

- [ ] **Step 2: Implement load query** (from spec):

```sql
SELECT c.id, c.address, c.abi, e.event_name, e.signature, e.topic0
FROM contracts c
INNER JOIN evm_event_signatures e ON e.contract_id = c.id
WHERE c.network_id = $1 AND c.status = 'active' AND e.status = 'active'
```

Build `AllowedTopic0ByAddr[address][topic0] = struct{}{}`.

- [ ] **Step 3: Implement indexer_config getters/setters**

```go
func (s *Store) GetConfigValue(ctx context.Context, networkID int64, key string) (string, error)
func (s *Store) SetCheckpoint(ctx context.Context, networkID int64, block uint64) error
```

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(evm): load indexer runtime config with event signatures"
```

---

## Task 8: RPC Client — `eth_getBlockReceipts` + Dual Fetch Paths

**Files:**
- Modify: `internal/chains/evm/client.go`
- Modify: `internal/chains/evm/rpcpool.go`
- Create: `internal/chains/evm/receipts.go`
- Create: `internal/chains/evm/receipts_test.go`
- Create: `internal/chains/evm/log_processor.go`
- Create: `internal/chains/evm/block_fetcher.go`
- Modify: `internal/chains/evm/fetcher.go`
- Create: `internal/chains/evm/fetcher_test.go`

**Interfaces:**
- Produces:

```go
// receipts.go
type RpcReceipt struct {
	TransactionHash common.Hash
	TransactionIndex uint
	Logs            []types.Log
}

func FlattenReceiptLogs(receipts []RpcReceipt) []types.Log
func GetBlockReceiptsWithRetry(ctx context.Context, client RPCClient, blockHash common.Hash, maxRetries int, retryDelay time.Duration) ([]RpcReceipt, error)

// client.go — raw RPC (go-ethereum Client.CallContext)
func (c *Client) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error)

// log_processor.go — shared by both fetch paths
type LogProcessor struct {
	allowedTopic0ByAddr map[string]map[string]struct{}
	abiByAddr           map[string]json.RawMessage
	eventNameByAddrTopic map[string]map[string]string // addr → topic0 → eventName
}

func (p *LogProcessor) ProcessLogs(logs []types.Log, isFinalized bool) []ProcessedLog

// block_fetcher.go — tip-following / genesis
func (f *BlockFetcher) FetchBlockLogs(ctx context.Context, blockHash common.Hash) ([]ProcessedLog, error)

// fetcher.go — catch-up batch (existing LogFetcher, extended)
func (f *LogFetcher) FetchLogsRange(ctx context.Context, fromBlock, toBlock uint64, addresses []common.Address) ([]ProcessedLog, error)
```

- [ ] **Step 1: Extend `RPCClient` interface in `rpcpool.go`**

```go
type RPCClient interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	FilterLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []common.Address) ([]types.Log, error)
	GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error)
}
```

Add `GetBlockReceipts` to `RPCPool` via `callWithFallback` (same pattern as `FilterLogs`).

- [ ] **Step 2: Implement `GetBlockReceipts` on `Client`**

Use `eth_getBlockReceipts` — not exposed by go-ethereum high-level API, call via `Client().Client().CallContext`:

```go
func (c *Client) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error) {
	var raw []struct {
		TransactionHash  common.Hash    `json:"transactionHash"`
		TransactionIndex hexutil.Uint   `json:"transactionIndex"`
		Logs             []types.Log    `json:"logs"`
	}
	err := c.Client.Client().CallContext(ctx, &raw, "eth_getBlockReceipts", blockHash)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil // block not ready — caller retries
	}
	out := make([]RpcReceipt, len(raw))
	for i, r := range raw {
		out[i] = RpcReceipt{
			TransactionHash:  r.TransactionHash,
			TransactionIndex: uint(r.TransactionIndex),
			Logs:             r.Logs,
		}
	}
	return out, nil
}
```

- [ ] **Step 3: Write failing test for retry on null receipts**

```go
func TestGetBlockReceiptsWithRetry_RetriesUntilReady(t *testing.T) {
	calls := 0
	mock := &mockRPCClient{
		getBlockReceipts: func(ctx context.Context, hash common.Hash) ([]RpcReceipt, error) {
			calls++
			if calls < 3 {
				return nil, nil
			}
			return []RpcReceipt{{Logs: []types.Log{{}}}}, nil
		},
	}
	_, err := GetBlockReceiptsWithRetry(context.Background(), mock, common.Hash{}, 5, time.Millisecond)
	if err != nil || calls != 3 {
		t.Fatalf("want 3 calls, got %d err=%v", calls, err)
	}
}
```

- [ ] **Step 4: Implement `LogProcessor` with topic0 filter**

Shared filter used by **both** `eth_getLogs` and `eth_getBlockReceipts` paths:

```go
for _, log := range logs {
	addr := strings.ToLower(log.Address.Hex())
	topic0 := ""
	if len(log.Topics) > 0 {
		topic0 = strings.ToLower(log.Topics[0].Hex())
	}
	if allowed, ok := p.allowedTopic0ByAddr[addr]; !ok {
		continue
	} else if _, ok := allowed[topic0]; !ok {
		continue
	}
	// decode + append ProcessedLog
}
```

- [ ] **Step 5: Implement `BlockFetcher.FetchBlockLogs`**

```go
func (f *BlockFetcher) FetchBlockLogs(ctx context.Context, blockHash common.Hash) ([]ProcessedLog, error) {
	receipts, err := GetBlockReceiptsWithRetry(ctx, f.client, blockHash, 5, f.retryDelay)
	if err != nil {
		return nil, err
	}
	logs := FlattenReceiptLogs(receipts)
	return f.processor.ProcessLogs(logs, false), nil
}
```

- [ ] **Step 6: Extend `LogFetcher` for catch-up range**

Keep existing per-block `FilterLogs` for Mongo path; add `FetchLogsRange` for postgres catch-up:

```go
func (f *LogFetcher) FetchLogsRange(ctx context.Context, from, to uint64, addresses []common.Address) ([]ProcessedLog, error) {
	fromBI := new(big.Int).SetUint64(from)
	toBI := new(big.Int).SetUint64(to)
	logs, err := f.client.FilterLogs(ctx, fromBI, toBI, addresses)
	if err != nil {
		return nil, err
	}
	return f.processor.ProcessLogs(logs, true), nil
}
```

- [ ] **Step 7: Run tests and commit**

```bash
go test ./internal/chains/evm/... -run 'TestGetBlockReceipts|TestFlatten|TestFetchLogs' -v
git add internal/chains/evm/
git commit -m "feat(evm): add getBlockReceipts, dual fetch paths, shared log processor"
```

---

## Task 9: RPC Pool Wiring (Postgres path)

**Files:**
- Modify: `cmd/indexer/main.go`

- [ ] **Step 1: Add `runEVMPostgres` function**

```go
func runEVMPostgres(cfg config.Config) {
	ctx := context.Background()
	pgStore, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil { log.Fatal(err) }
	defer pgStore.Close()

	runtimeCfg, err := pgStore.LoadIndexerRuntimeConfig(ctx)
	if err != nil { log.Fatal(err) }
	if !runtimeCfg.ScanEnabled {
		log.Fatal("scan_enabled is false")
	}
	if len(runtimeCfg.RPCURLs) == 0 {
		log.Fatal("network has no rpc_urls configured")
	}

	pool, err := evm.NewRPCPool(runtimeCfg.RPCURLs)
	if err != nil { log.Fatal(err) }
	log.Printf("rpc pool active: %s (%d urls)", pool.ActiveURL(), len(runtimeCfg.RPCURLs))

	// ... block indexer (Task 11) uses pool + runtimeCfg + BlockFetcher + LogFetcher
}
```

- [ ] **Step 2: Branch in `runEVM`**

```go
if cfg.StorageBackend == "postgres" {
	runEVMPostgres(cfg)
	return
}
// existing MongoDB path unchanged
```

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(indexer): wire EVM postgres path with RPC pool fallback"
```

---

## Task 10: Indexer Runtime Config (batch + tip-following)

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/storage/postgres/indexer_config.go`

Add env/config keys (mirror `indexer-service`):

| Key | Default | Description |
|-----|---------|-------------|
| `INDEXER_BLOCK_BATCH_SIZE` | `1000` | Max blocks per catch-up `eth_getLogs` call |
| `INDEXER_TIP_FOLLOWING_ENABLED` | `true` | When near head, scan block-by-block via receipts |
| `INDEXER_RETRY_AFTER_RECEIPTS_NULL` | `1s` | Sleep between `getBlockReceipts` retries |
| `INDEXER_SLEEP_AFTER_CATCHUP_BATCH` | `1s` | Pause after each catch-up batch |
| `INDEXER_NEW_BLOCK_WAITING_TIME` | `500ms` | Wait when next block not available |

- [ ] **Step 1: Add fields to `config.Config`**

- [ ] **Step 2: Expose via `IndexerRuntimeConfig`**

```go
type IndexerRuntimeConfig struct {
	// ... existing fields
	BlockBatchSize       uint64
	TipFollowingEnabled  bool
	RetryAfterReceiptsNull time.Duration
}
```

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(config): indexer batch size and tip-following settings"
```

---

## Task 11: Block Indexer — Dual-Mode Scan Loop & Transactional Persist

**Files:**
- Create: `internal/chains/evm/block_indexer.go`
- Create: `internal/chains/evm/decode.go`
- Create: `internal/chains/evm/block_indexer_test.go`

**Interfaces:**
- Consumes: `IndexerRuntimeConfig`, `RPCPool`, `*postgres.Store`
- Produces: `type BlockIndexer struct { ... }; func (b *BlockIndexer) Run(ctx context.Context) error`

- [ ] **Step 1: Implement decode helper**

```go
func DecodeLogParams(abiJSON json.RawMessage, eventName string, topics []string, data string) (map[string]any, error)
// numeric values → string; on error return nil, err (caller stores NULL decoded_params)
```

- [ ] **Step 2: Implement `PersistConfirmedBlock` transaction**

Single `pgx.Tx`:

1. Upsert `blocks` row (`ON CONFLICT (network_id, block_hash) DO UPDATE`)
2. `INSERT INTO eth_logs ... ON CONFLICT DO NOTHING RETURNING id`
3. `INSERT INTO outbox (log_id) SELECT ... WHERE NOT EXISTS`
4. `UPDATE indexer_config SET value = (block+1) WHERE key = 'checkpoint'`

Only call when `currentHead - blockNumber >= confirmation_depth`.

- [ ] **Step 3: Implement dual-mode scan loop**

```go
for {
	if !runtimeCfg.ScanEnabled { sleep; continue }
	head, _ := pool.HeaderByNumber(ctx, nil)
	checkpoint := runtimeCfg.Checkpoint

	if checkpoint == 0 {
		// Genesis: latest block via getBlockReceipts
		block, _ := pool.BlockByNumber(ctx, head.Number)
		logs, _ := blockFetcher.FetchBlockLogs(ctx, block.Hash())
		persistIfConfirmed(...)
		continue
	}

	if head.Number.Uint64()-uint64(runtimeCfg.ConfirmationDepth) > checkpoint {
		// Catch-up: eth_getLogs batch
		to := min(checkpoint+runtimeCfg.BlockBatchSize, head-confirmations)
		logs, _ := logFetcher.FetchLogsRange(ctx, checkpoint+1, to, addresses)
		persistBatch(...)
		sleep(cfg.SleepAfterCatchupBatch)
		continue
	}

	if runtimeCfg.TipFollowingEnabled {
		// Tip-following: one block via getBlockReceipts
		next := checkpoint + 1
		block, err := pool.BlockByNumber(ctx, big.NewInt(int64(next)))
		if err != nil || block == nil {
			sleep(cfg.NewBlockWaitingTime)
			continue
		}
		// reorg check: compare parentHash vs blocks table
		logs, _ := blockFetcher.FetchBlockLogs(ctx, block.Hash())
		persistBlock(...)
		continue
	}

	sleep(cfg.PollIntervalWhenConfirmed)
}
```

- [ ] **Step 4: Write integration test** (testcontainers postgres or manual script)

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(evm): dual-mode indexer with getBlockReceipts at tip and getLogs catch-up"
```

---

## Task 12: Outbox Consumer

**Files:**
- Create: `cmd/consumer/main.go`
- Create: `internal/consumer/outbox.go`
- Create: `internal/consumer/webhook.go`

**Interfaces:**
- Produces: `func PollAndDeliver(ctx context.Context, store *postgres.Store, endpoint WebhookEndpoint) error`

- [ ] **Step 1: Implement SKIP LOCKED poll** (from spec)

- [ ] **Step 2: Build webhook payload** from `eth_logs` join

- [ ] **Step 3: On success** — `outbox.status = 'delivered'`, insert `webhook_deliveries`

- [ ] **Step 4: On failure** — increment `attempts`, exponential `next_retry_at`, `last_error`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(consumer): outbox poller with webhook delivery"
```

---

## Task 13: Documentation & End-to-End Test

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document Postgres quickstart**

```bash
docker compose up -d postgres
go run ./cmd/migrate -- up
STORAGE_BACKEND=postgres go run ./cmd/seed-pg --config cmd/seed/data/base.json
STORAGE_BACKEND=postgres CHAIN_TYPE=evm go run ./cmd/indexer
go run ./cmd/consumer
```

- [ ] **Step 2: Document operator workflows**

- Stop/start: `UPDATE indexer_config SET value = 'false' WHERE key = 'scan_enabled'`
- Disable event: `UPDATE evm_event_signatures SET status = 'deactive' WHERE ...`
- Restart indexer after config change

- [ ] **Step 3: Manual E2E checklist**

| Step | Check |
|------|-------|
| Seed | `evm_event_signatures` rows exist with correct `topic0` |
| Indexer start | Log shows `rpc pool active: <url>` |
| Scan 1 block | `eth_logs` row inserted, `decoded_params` populated |
| Confirmed block | `outbox` row created |
| Consumer | `webhook_deliveries` row, `outbox.status = delivered` |
| Tip-following | `getBlockReceipts` on block N+1, receipts retry on null |
| Catch-up batch | `eth_getLogs` with address filter, batch ≤ 1000 blocks |
| RPC failover | Kill primary RPC → log `rpc pool: switched to` |
| Disable event | Set `deactive`, restart → no new logs for that topic0 |

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: PostgreSQL indexer setup and operator guide"
```

---

## Spec Coverage Checklist

| Spec section | Task |
|--------------|------|
| `networks.rpc_urls` JSONB array | 3, 4, 6, 9 |
| RPC pool fallback | 8, 9 (reuse `evm/rpcpool.go`) |
| `indexer_config` checkpoint / scan_enabled | 3, 6, 7, 10, 11 |
| `contracts` without `events[]` | 3, 5 |
| `evm_event_signatures` | 3, 5, 6, 7 |
| `eth_getBlockReceipts` (tip/genesis) | 8, 11 |
| `eth_getLogs` catch-up batch | 8, 11 |
| topic0 filter in-memory | 8 |
| tip-following + receipts retry | 8, 10, 11 |
| `blocks` parent linkage + reorg cascade | 3, 11 |
| `eth_logs` + `decoded_params` | 11 |
| Transactional `outbox` | 11 |
| `webhook_endpoints` + `webhook_deliveries` | 12 |
| MongoDB migration notes | 6 (seed-pg from same JSON) |

## Out of Scope (follow-up plans)

| Item | Reason |
|------|--------|
| Solana PostgreSQL path | Separate `solana_event_signatures` design needed |
| Factory dynamic contract discovery | Port from `indexer-service` spawner pattern; `getBlockReceipts` in-memory re-scan is prerequisite |
| Hot-reload `evm_event_signatures` | Spec says restart worker for now |
| MongoDB removal | Keep dual-backend until Postgres path is stable |
| `reorg_events` audit table | Marked optional in spec |

---

## Execution Order Summary

```text
Task 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13
         └─ migrations must exist before storage tests
              └─ seed before indexer config load tests
                   └─ getBlockReceipts + getLogs (Task 8) before block indexer loop
                        └─ block indexer before consumer E2E
```
