# Go Indexing

EVM on-chain event indexer backed by **PostgreSQL**.

## Data model

- One indexer process ↔ one PostgreSQL database (`POSTGRES_DSN`).
- Config: `networks`, `contracts`, `evm_event_signatures`, `indexer_config`.
- Indexed data: `blocks`, `eth_logs`, `outbox`.
- **RPC** comes from `networks.rpc_urls` (seed), not from env.

## Requirements

- Go 1.25+
- Docker (PostgreSQL)
- Optional `.env` — copy from `.env.example`

## Quick start

### 1. PostgreSQL + migrate

```bash
docker compose up -d postgres
go run ./cmd/migrate -- up
```

### 2. Seed

```bash
go run ./cmd/seed-pg --config cmd/seed/data/base.json
```

Default checkpoint = `min(contracts[].start_block)`. Override: `--checkpoint 12345678`.

### 3. Run indexer

```powershell
$env:CHAIN_NAME="base"
go run ./cmd/indexer/main.go
```

### 4. Run consumer (optional webhook)

```powershell
$env:WEBHOOK_URL="https://your-webhook.example/hooks"
$env:WEBHOOK_SECRET="your-secret"
go run ./cmd/consumer/main.go
```

**Log fetch strategy:**
- **Catch-up**: `eth_getLogs` + filter `address`
- **Tip-following / genesis**: `eth_getBlockReceipts` → filter `topic0` in-memory

Event listen config lives in `evm_event_signatures`.

### Common errors

| Message | Fix |
|---------|-----|
| `no active contracts with evm_event_signatures` | Run `seed-pg` or enable contract/event rows. |
| `scan_enabled is false` | `UPDATE indexer_config SET value = 'true' WHERE key = 'scan_enabled'`. |
| Cannot connect to PostgreSQL | Check `POSTGRES_DSN`, `docker compose up -d postgres`. |

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CHAIN_NAME` | `evm` | Label in logs. |
| `POSTGRES_DSN` | `postgres://indexer:indexer@localhost:5432/indexing?sslmode=disable` | PostgreSQL DSN. |
| `INDEXER_BLOCK_BATCH_SIZE` | `100` | Catch-up batch (`eth_getLogs`). Auto-halves on RPC errors. |
| `INDEXER_TIP_FOLLOWING_ENABLED` | `true` | Tip-following via `eth_getBlockReceipts`. |
| `INDEXER_RETRY_AFTER_RECEIPTS_NULL` | `1s` | Retry when receipts are not ready yet. |
| `WEBHOOK_URL` / `WEBHOOK_SECRET` | — | Consumer webhook. |
| `CONSUMER_ID` | `consumer-1` | Consumer worker ID. |

The indexer uses an **RPC pool**: it tries `network.rpc_urls` in order on failure.

## Multi-chain deployment

Each chain = separate PostgreSQL database (or cluster) + one indexer process.

```bash
# Base
POSTGRES_DSN=postgres://.../indexing_base go run ./cmd/seed-pg --config cmd/seed/data/base.json
POSTGRES_DSN=postgres://.../indexing_base CHAIN_NAME=base go run ./cmd/indexer/main.go

# Sepolia
POSTGRES_DSN=postgres://.../indexing_sepolia go run ./cmd/seed-pg --config cmd/seed/data/sepolia.json
POSTGRES_DSN=postgres://.../indexing_sepolia CHAIN_NAME=sepolia go run ./cmd/indexer/main.go
```

## Add a new EVM contract

1. Edit JSON in `cmd/seed/data/`.
2. `go run ./cmd/seed-pg --config ...` (upserts contract + `evm_event_signatures`).
3. Restart indexer.

```json
{
  "network": { "name": "...", "chain_type": "evm", "chain_id": 8453, "rpc_urls": ["https://..."], "type": "mainnet", "status": "active" },
  "contracts": [{
    "address": "0x...",
    "abi": [],
    "events": ["Transfer(address,address,uint256)"],
    "start_block": 12345678
  }]
}
```

## Layout

```
cmd/
  indexer/          # EVM block indexer entry point
  seed-pg/          # Seed PostgreSQL from JSON
  seed/data/        # Per-chain seed JSON
  migrate/          # Goose migrations
  consumer/         # Outbox → webhook
internal/
  chains/evm/       # Block indexer, RPC pool, log processor
  storage/postgres/ # PostgreSQL store
migrations/         # SQL schema
```

## Test

```bash
go test ./... -v
```
