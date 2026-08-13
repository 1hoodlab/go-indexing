# PostgreSQL Indexer, Queue & Webhook Architecture

> Transactional outbox pattern: save Ethereum logs and enqueue in one PostgreSQL transaction.
> Consumer delivers to external webhook service. Blocks table handles reorg via parent chain.

## Decisions

| Decision | Choice |
|----------|--------|
| Database | PostgreSQL (replace MongoDB for indexer path) |
| Queue | In-DB `outbox` table (transactional outbox) |
| Event listen config | `evm_event_signatures` table (per contract, per topic0); not embedded on `contracts` |
| Topics storage | `JSONB` array on `eth_logs`; generated `topic0` column for indexes |
| Decoded data | `decoded_params JSONB` on `eth_logs` (ABI decode at index time) |
| Scan control | `indexer_config` key/value table (stop/start, checkpoint) |
| Scan anchor | `indexer_config.checkpoint` only — no `start_block` on contracts |
| Reorg | `blocks` table with `parent_block_id`; cascade to logs + outbox |
| Webhook idempotency | Dedup on `eth_logs`; skip outbox insert if `log_id` exists |

## Entity Relationship

```mermaid
erDiagram
    networks ||--o{ indexer_config : "config"
    networks ||--o{ contracts : "has many"
    networks ||--o{ blocks : "tracks"
    networks ||--o{ eth_logs : "indexed on"
    blocks ||--o{ blocks : "parent_block_id"
    blocks ||--o{ eth_logs : "contains"
    contracts ||--o{ evm_event_signatures : "listens to"
    contracts ||--o{ eth_logs : "emits"
    eth_logs ||--o| outbox : "1:1 enqueue"
    outbox ||--o{ webhook_deliveries : "attempts"
    webhook_endpoints ||--o{ webhook_deliveries : "target"

    networks {
        bigint id PK
        bigint chain_id UK
        text name
        text chain_type
        text status
    }

    indexer_config {
        bigint id PK
        bigint network_id FK
        text key UK
        text value
        text description
    }

    contracts {
        bigint id PK
        bigint network_id FK
        text address UK
        jsonb abi
        text status
    }

    evm_event_signatures {
        bigint id PK
        bigint contract_id FK
        text event_name
        text signature
        text topic0
        text status
    }

    blocks {
        bigint id PK
        bigint network_id FK
        bigint block_number
        text block_hash UK
        bigint parent_block_id FK
        text status
        boolean is_canonical
    }

    eth_logs {
        bigint id PK
        bigint block_id FK
        text event_name
        jsonb topics
        text topic0 GENERATED
        text data
        jsonb decoded_params
        text status
    }

    outbox {
        bigint id PK
        bigint log_id FK UK
        text status
        int attempts
    }

    webhook_endpoints {
        bigint id PK
        text url
        boolean enabled
    }

    webhook_deliveries {
        bigint id PK
        bigint outbox_id FK
        bigint endpoint_id FK
    }
```

---

## Full SQL Schema

### Extensions

```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- optional, for gen_random_uuid if using UUIDs
```

### Enums

```sql
CREATE TYPE network_status AS ENUM ('active', 'deactive');
CREATE TYPE contract_status AS ENUM ('active', 'deactive');
CREATE TYPE block_status AS ENUM ('pending', 'confirmed', 'orphaned');
CREATE TYPE log_status AS ENUM ('confirmed', 'orphaned');
CREATE TYPE outbox_status AS ENUM ('pending', 'processing', 'delivered', 'failed', 'cancelled');
```

---

### 1. `networks`

Blockchain network configuration (RPC URLs, chain identity).

```sql
CREATE TABLE networks (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    chain_id    BIGINT NOT NULL,
    chain_type  TEXT NOT NULL DEFAULT 'evm',          -- evm | solana | ...
    network_type TEXT NOT NULL DEFAULT 'mainnet',     -- mainnet | testnet
    rpc_urls    JSONB NOT NULL DEFAULT '[]',          -- ["https://...", ...]
    status      network_status NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT networks_chain_id_unique UNIQUE (chain_id),
    CONSTRAINT networks_rpc_urls_is_array CHECK (jsonb_typeof(rpc_urls) = 'array')
);

CREATE INDEX idx_networks_status ON networks (status);
```

---

### 2. `indexer_config`

Key/value scan control per network. Operator stops indexer, sets checkpoint, starts again.

```sql
CREATE TABLE indexer_config (
    id          BIGSERIAL PRIMARY KEY,
    network_id  BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT indexer_config_network_key_unique UNIQUE (network_id, key)
);

CREATE INDEX idx_indexer_config_network ON indexer_config (network_id);
```

**Seed keys per network:**

| key | default | description |
|-----|---------|-------------|
| `scan_enabled` | `true` | Stop/start switch. `false` = indexer idle. |
| `checkpoint` | set at seed | Next block number to scan. Single anchor for the network. |
| `confirmation_depth` | `12` | Blocks before block is confirmed and outbox rows are created. |

```sql
-- Example bootstrap (run after network insert)
-- checkpoint value comes from seed JSON (e.g. 12345678), NOT from contracts
INSERT INTO indexer_config (network_id, key, value, description) VALUES
    ($network_id, 'scan_enabled', 'true', 'Stop/start switch. false = indexer idle.'),
    ($network_id, 'checkpoint', '12345678', 'Next block number to scan.'),
    ($network_id, 'confirmation_depth', '12', 'Blocks before confirm + outbox enqueue.');
```

**Scan anchoring:** Block scanning is anchored only by `indexer_config.checkpoint` (network-level). `contracts` + `evm_event_signatures` define *what* to index (address, ABI, which topic0s to listen to), not *where* to start. To index from an earlier block — including lookback or a newly added contract — stop the indexer and set `checkpoint` to the desired block.

**Operator workflow:**

```sql
-- Stop
UPDATE indexer_config SET value = 'false', updated_at = now()
WHERE network_id = $1 AND key = 'scan_enabled';

-- Rewind (e.g. block 14900)
UPDATE indexer_config SET value = '14900', updated_at = now()
WHERE network_id = $1 AND key = 'checkpoint';

-- Start
UPDATE indexer_config SET value = 'true', updated_at = now()
WHERE network_id = $1 AND key = 'scan_enabled';
```

---

### 3. `contracts`

EVM contracts to index on a network. Event listen targets live in `evm_event_signatures` (not embedded here).

```sql
CREATE TABLE contracts (
    id          BIGSERIAL PRIMARY KEY,
    network_id  BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    address     TEXT NOT NULL,                        -- lowercase 0x-prefixed
    abi         JSONB,                                -- minified ABI JSON
    status      contract_status NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT contracts_network_address_unique UNIQUE (network_id, address),
    CONSTRAINT contracts_address_format CHECK (address ~ '^0x[0-9a-f]{40}$')
);

CREATE INDEX idx_contracts_network ON contracts (network_id);
CREATE INDEX idx_contracts_status ON contracts (network_id, status);
```

---

### 4. `evm_event_signatures`

Which EVM events the indexer listens to per contract. Loaded at worker start; changes require indexer restart.

Each row stores the canonical event signature string and its precomputed `topic0` (keccak256 hash). The indexer filters RPC logs by `topic0` and only persists logs whose `topic0` matches an active row.

```sql
CREATE TABLE evm_event_signatures (
    id           BIGSERIAL PRIMARY KEY,
    contract_id  BIGINT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    event_name   TEXT NOT NULL,                       -- e.g. "Transfer"
    signature    TEXT NOT NULL,                       -- e.g. "Transfer(address,address,uint256)"
    topic0       TEXT NOT NULL,                       -- keccak256(signature), lowercase 0x-prefixed
    status       contract_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT evm_event_signatures_contract_topic0_unique UNIQUE (contract_id, topic0),
    CONSTRAINT evm_event_signatures_topic0_format CHECK (topic0 ~ '^0x[0-9a-f]{64}$')
);

CREATE INDEX idx_evm_event_signatures_contract ON evm_event_signatures (contract_id);
CREATE INDEX idx_evm_event_signatures_topic0 ON evm_event_signatures (topic0);
CREATE INDEX idx_evm_event_signatures_active
    ON evm_event_signatures (contract_id, status)
    WHERE status = 'active';
```

**Indexer load query** (equivalent to `indexer-service` `contract` + `event_signature` join):

```sql
SELECT
    c.id            AS contract_id,
    c.address,
    c.abi,
    e.event_name,
    e.signature,
    e.topic0
FROM contracts c
INNER JOIN evm_event_signatures e ON e.contract_id = c.id
WHERE c.network_id = $1
  AND c.status = 'active'
  AND e.status = 'active';
```

**Seed from JSON** (`events: ["Transfer(address,address,uint256)", ...]`):

1. Upsert `contracts` row (address + abi).
2. For each event string: compute `topic0` in application code (keccak256), derive `event_name` from signature, insert into `evm_event_signatures` with `ON CONFLICT (contract_id, topic0) DO NOTHING`.

**Operator workflow — stop listening to one event:**

```sql
UPDATE evm_event_signatures
SET status = 'deactive', updated_at = now()
WHERE contract_id = $1 AND topic0 = $2;
-- restart indexer worker
```

**Re-enable:**

```sql
UPDATE evm_event_signatures
SET status = 'active', updated_at = now()
WHERE contract_id = $1 AND topic0 = $2;
-- restart indexer worker
```

**Dynamic contract discovery** (factory spawns child contract): upsert `contracts` + bulk-insert new `evm_event_signatures` rows in a separate transaction, then restart indexer (or hot-reload when implemented).

**Indexer fetch filter:** At startup, build an in-memory map `address → set(topic0)` from the load query above. For each block:

1. Call `eth_getLogs` with contract addresses (optionally pass `topics: [[topic0_a, topic0_b, ...]]` to filter at RPC).
2. Drop any log whose `topic0` is not in the allowed set for that contract address.
3. Decode and insert only matching logs into `eth_logs`.

Config is loaded once at worker start — same as `indexer-service`. After changing `evm_event_signatures` in the database, restart the indexer.

---

### 5. `blocks`

Block chain with parent linkage for reorg detection and cascade.

```sql
CREATE TABLE blocks (
    id                  BIGSERIAL PRIMARY KEY,
    network_id          BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    chain_id            BIGINT NOT NULL,
    block_number        BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    parent_hash         TEXT,                         -- from RPC
    parent_block_id     BIGINT REFERENCES blocks (id),
    block_timestamp     TIMESTAMPTZ NOT NULL,
    status              block_status NOT NULL DEFAULT 'pending',
    is_canonical        BOOLEAN NOT NULL DEFAULT true,
    confirmation_depth  INT,                          -- depth when confirmed
    confirmed_at        TIMESTAMPTZ,
    orphaned_at         TIMESTAMPTZ,
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT blocks_network_hash_unique UNIQUE (network_id, block_hash),
    CONSTRAINT blocks_hash_format CHECK (block_hash ~ '^0x[0-9a-f]{64}$')
);

-- Only one canonical block per height per network
CREATE UNIQUE INDEX idx_blocks_canonical_height
    ON blocks (network_id, block_number)
    WHERE is_canonical = true;

CREATE INDEX idx_blocks_network_number ON blocks (network_id, block_number DESC);
CREATE INDEX idx_blocks_parent_hash ON blocks (network_id, parent_hash);
CREATE INDEX idx_blocks_status ON blocks (network_id, status);
CREATE INDEX idx_blocks_head ON blocks (network_id, is_canonical, block_number DESC)
    WHERE is_canonical = true;
```

**Reorg cascade (single transaction):**

```sql
-- 1. Mark forked blocks orphaned
UPDATE blocks SET status = 'orphaned', is_canonical = false, orphaned_at = now()
WHERE network_id = $1 AND block_number >= $fork_height AND block_hash = $old_hash;

-- 2. Cascade logs
UPDATE eth_logs SET status = 'orphaned'
WHERE block_id IN (SELECT id FROM blocks WHERE ... orphaned ...);

-- 3. Cancel pending outbox
UPDATE outbox SET status = 'cancelled'
WHERE log_id IN (SELECT id FROM eth_logs WHERE status = 'orphaned')
  AND status IN ('pending', 'processing');
```

---

### 6. `eth_logs`

Indexed Ethereum event logs. Raw on-chain fields plus ABI-decoded params on the same row.

```sql
CREATE TABLE eth_logs (
    id                  BIGSERIAL PRIMARY KEY,
    block_id            BIGINT NOT NULL REFERENCES blocks (id),
    network_id          BIGINT NOT NULL REFERENCES networks (id),
    chain_id            BIGINT NOT NULL,
    contract_id         BIGINT NOT NULL REFERENCES contracts (id),
    contract_address    TEXT NOT NULL,
    block_number        BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    transaction_hash    TEXT NOT NULL,
    transaction_index   INT,
    log_index           INT NOT NULL,
    event_name          TEXT,                         -- from ABI lookup at index time
    topics              JSONB NOT NULL DEFAULT '[]',
    topic0              TEXT GENERATED ALWAYS AS (topics->>0) STORED,
    data                TEXT NOT NULL DEFAULT '0x',   -- non-indexed hex data
    decoded_params      JSONB,                        -- ABI-decoded params; NULL if decode failed
    status              log_status NOT NULL DEFAULT 'confirmed',
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT eth_logs_dedup_unique
        UNIQUE (chain_id, block_number, transaction_hash, log_index),
    CONSTRAINT eth_logs_topics_is_array CHECK (jsonb_typeof(topics) = 'array'),
    CONSTRAINT eth_logs_topics_length CHECK (jsonb_array_length(topics) BETWEEN 0 AND 4),
    CONSTRAINT eth_logs_decoded_params_is_object
        CHECK (decoded_params IS NULL OR jsonb_typeof(decoded_params) = 'object'),
    CONSTRAINT eth_logs_contract_address_format CHECK (contract_address ~ '^0x[0-9a-f]{40}$')
);

CREATE INDEX idx_eth_logs_block ON eth_logs (block_id);
CREATE INDEX idx_eth_logs_network ON eth_logs (network_id, indexed_at DESC);
CREATE INDEX idx_eth_logs_contract_event ON eth_logs (contract_address, topic0, block_number DESC);
CREATE INDEX idx_eth_logs_block_number ON eth_logs (chain_id, block_number DESC);
CREATE INDEX idx_eth_logs_tx ON eth_logs (transaction_hash);
CREATE INDEX idx_eth_logs_status ON eth_logs (status) WHERE status = 'confirmed';
CREATE INDEX idx_eth_logs_topics_gin ON eth_logs USING GIN (topics jsonb_path_ops);
CREATE INDEX idx_eth_logs_decoded_params_gin ON eth_logs USING GIN (decoded_params jsonb_path_ops);
```

**Topics JSONB shape:**

```json
[
  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
  "0x000000000000000000000000from...",
  "0x000000000000000000000000to..."
]
```

- Index `0` = topic0 (event signature hash)
- Length 1–4; no null padding
- Lowercase hex with `0x` prefix

**Decoded params JSONB shape:**

Indexer decodes using `contracts.abi` + `evm_event_signatures.event_name` + `topics` + `data` at insert time (same logic as current `SaveToLogs` handler):

```json
{
  "from": "0xabc...",
  "to": "0xdef...",
  "value": "1000000000000000000"
}
```

- `NULL` when ABI missing, event name unknown, or decode error
- Numeric values stored as **strings** (preserve uint256 precision)
- Decode failure does **not** block raw log insert or outbox enqueue

---

### 7. `outbox`

Transactional queue: one row per log, created in same TX as `eth_logs` insert (when block is confirmed).

```sql
CREATE TABLE outbox (
    id              BIGSERIAL PRIMARY KEY,
    log_id          BIGINT NOT NULL REFERENCES eth_logs (id) ON DELETE RESTRICT,
    status          outbox_status NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 5,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,                             -- consumer worker id
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,

    CONSTRAINT outbox_log_unique UNIQUE (log_id)
);

CREATE INDEX idx_outbox_pending ON outbox (next_retry_at)
    WHERE status = 'pending';
CREATE INDEX idx_outbox_processing ON outbox (locked_at)
    WHERE status = 'processing';
```

**Consumer poll (SKIP LOCKED):**

```sql
SELECT o.id, o.log_id
FROM outbox o
WHERE o.status = 'pending'
  AND o.next_retry_at <= now()
ORDER BY o.id
FOR UPDATE SKIP LOCKED
LIMIT 100;
```

---

### 8. `webhook_endpoints`

External webhook targets.

```sql
CREATE TABLE webhook_endpoints (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL,
    secret      TEXT,                                 -- HMAC signing secret
    headers     JSONB NOT NULL DEFAULT '{}',          -- extra HTTP headers
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

### 9. `webhook_deliveries`

Audit trail of delivery attempts (retries).

```sql
CREATE TABLE webhook_deliveries (
    id              BIGSERIAL PRIMARY KEY,
    outbox_id       BIGINT NOT NULL REFERENCES outbox (id) ON DELETE CASCADE,
    endpoint_id     BIGINT NOT NULL REFERENCES webhook_endpoints (id),
    http_status     INT,
    response_body   TEXT,
    error_message   TEXT,
    duration_ms     INT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_webhook_deliveries_outbox ON webhook_deliveries (outbox_id);
CREATE INDEX idx_webhook_deliveries_attempted ON webhook_deliveries (attempted_at DESC);
```

---

## Indexer Transaction (confirmed block)

When a block reaches `confirmation_depth`, in one transaction:

Only logs whose `topic0` matches an active `evm_event_signatures` row are inserted (filtered at fetch time or before insert).

```sql
BEGIN;

-- 1. Insert or skip block
INSERT INTO blocks (network_id, chain_id, block_number, block_hash, parent_hash, parent_block_id, block_timestamp, status, is_canonical)
VALUES (...)
ON CONFLICT (network_id, block_hash) DO UPDATE SET status = 'confirmed', confirmed_at = now()
RETURNING id;

-- 2. Insert logs with decoded params (skip duplicates on re-scan / checkpoint rewind)
INSERT INTO eth_logs (block_id, network_id, chain_id, contract_id, contract_address,
                      block_number, block_hash, block_timestamp, transaction_hash,
                      transaction_index, log_index, event_name, topics, data, decoded_params)
VALUES (...)
ON CONFLICT (chain_id, block_number, transaction_hash, log_index) DO NOTHING
RETURNING id;

-- 3. Enqueue only new logs (idempotent — no double webhooks on checkpoint rewind)
INSERT INTO outbox (log_id, status)
SELECT l.id, 'pending'
FROM eth_logs l
WHERE l.id = ANY($new_log_ids)
  AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.log_id = l.id);

-- 4. Advance checkpoint
UPDATE indexer_config SET value = ($block_number + 1)::text, updated_at = now()
WHERE network_id = $network_id AND key = 'checkpoint';

COMMIT;
```

**Pending blocks:** Insert into `blocks` with `status = 'pending'` and logs optionally, but **do not** insert `outbox` until `status = 'confirmed'`.

---

## Consumer Flow

```text
1. Poll outbox (SKIP LOCKED)
2. JOIN eth_logs to build payload (raw + decoded_params on same row)
3. POST to webhook_endpoints.url
4. On success: outbox.status = 'delivered', delivered_at = now()
5. On failure: attempts++, next_retry_at = backoff, status = 'failed' or 'pending'
6. INSERT webhook_deliveries audit row
```

**Webhook payload (built from join):**

```json
{
  "log_id": 12345,
  "chain_id": 8453,
  "network": "Base",
  "block_number": 12345678,
  "block_hash": "0x...",
  "block_timestamp": "2026-07-24T12:00:00Z",
  "transaction_hash": "0x...",
  "log_index": 42,
  "contract_address": "0x...",
  "event_name": "Transfer",
  "topics": ["0xddf252ad...", "0x...from", "0x...to"],
  "data": "0x...",
  "decoded_params": {
    "from": "0xabc...",
    "to": "0xdef...",
    "value": "1000000000000000000"
  }
}
```

---

## Table Summary

| Table | Purpose | Written by |
|-------|---------|------------|
| `networks` | Chain config, RPC URLs | Seed / admin |
| `indexer_config` | Stop/start, checkpoint, depth | Operator / indexer |
| `contracts` | Contract addresses and ABI | Seed / admin / discovery |
| `evm_event_signatures` | Per-contract event listen config (topic0) | Seed / admin / discovery |
| `blocks` | Block chain, parent linkage, reorg | Indexer |
| `eth_logs` | Raw logs + `decoded_params` JSONB | Indexer |
| `outbox` | Delivery queue | Indexer (same TX as logs) |
| `webhook_endpoints` | Target URLs | Admin |
| `webhook_deliveries` | Retry audit | Consumer |

---

## Component Architecture

```text
┌─────────────┐     RPC      ┌──────────────┐
│   Indexer   │◄────────────►│  Blockchain  │
│  (process)  │              └──────────────┘
└──────┬──────┘
       │ read: networks, contracts, evm_event_signatures, indexer_config
       │ write (TX): blocks, eth_logs, outbox, checkpoint
       ▼
┌─────────────────────────────────────────────┐
│              PostgreSQL                      │
│  networks → contracts → evm_event_signatures│
│  indexer_config    blocks → eth_logs → outbox│
└──────────────────┬──────────────────────────┘
                   │ poll SKIP LOCKED
                   ▼
            ┌─────────────┐     HTTP POST    ┌─────────────────┐
            │  Consumer   │─────────────────►│ Webhook Service │
            │  (process)  │                  │   (external)    │
            └─────────────┘                  └─────────────────┘
                   │ write: outbox status, webhook_deliveries
```

---

## Optional Future Tables

| Table | Purpose |
|-------|---------|
| `reorg_events` | Audit log when reorg cascade runs |

---

## Migration Notes (from current MongoDB)

| MongoDB collection | PostgreSQL table |
|--------------------|------------------|
| `networks` | `networks` |
| `contracts` | `contracts` (without embedded `events[]`) |
| `contracts.events[]` | `evm_event_signatures` (one row per event signature) |
| `logs` | `eth_logs` (raw fields) |
| `decoded_events` | `eth_logs.decoded_params` (merged into same row) |
| — | `blocks`, `indexer_config`, `evm_event_signatures`, `outbox`, `webhook_*` (new) |

**`contracts.events[]` migration:** For each contract document, unnest `events[]`, compute `topic0` per signature string in application code, insert rows into `evm_event_signatures`. Drop the `events` field from contract documents after migration.

Multi-chain deployment stays the same: **one PostgreSQL database per chain instance** (or schema per chain in one cluster).
