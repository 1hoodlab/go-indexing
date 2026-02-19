# Schema Design - Blockchain Indexer

## Overview

Design philosophy:
- **1 network** has **many contracts**
- Each contract can be enabled/disabled independently (active/deactive)
- All logs are stored with references to `network` and `contract` for traceability

---

## 1. Network Config Schema

Stores blockchain networks: RPC endpoints, type (mainnet/testnet), and status.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `name` | string | Network name (e.g. "Ethereum", "Base") |
| `chain_id` | int64 | Chain ID |
| `type` | enum | `mainnet` \| `testnet` |
| `rpc_urls` | []string | List of RPC endpoints (primary + fallbacks) |
| `status` | enum | `active` \| `deactive` |
| `created_at` | datetime | |
| `updated_at` | datetime | |

**Indexes:**
- `chain_id` (unique)
- `status` (for filtering active networks)

---

## 2. Contract Schema

Stores contracts to index. Each contract belongs to one network. Controls which contracts are scanned and tracks progress.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `network_id` | ObjectId | Reference to Network (1 contract → 1 network) |
| `address` | string | Contract address (lowercase, 0x-prefix) |
| `abi` | object/string | ABI JSON. **Minify before store** (remove spaces/newlines) to reduce size |
| `events` | []string | Event signatures to index (e.g. `Transfer(address,address,uint256)`) |
| `start_block` | uint64 | Block to start scanning from |
| `last_scanned_block` | uint64 | Last block that was indexed. **If 0 → start from `start_block`** |
| `status` | enum | `active` \| `deactive` (enable/disable scanning) |
| `created_at` | datetime | |
| `updated_at` | datetime | |

**Scanning logic:**
```
if last_scanned_block == 0:
    next_block = start_block
else:
    next_block = last_scanned_block + 1
```

**ABI storage:** Minify ABI (strip whitespace, newlines) before persisting to reduce document size.

**Indexes:**
- `network_id` + `address` (unique, composite)
- `status` (filter active contracts)
- `network_id` (query all contracts of a network)

---

## 3. Logs Schema

Stores all indexed event logs. Each log must be traceable to its network and contract.

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | Auto-generated |
| `network_id` | ObjectId | Reference to Network |
| `contract_id` | ObjectId | Reference to Contract |
| `block_number` | uint64 | Block number |
| `block_hash` | string | Block hash |
| `transaction_hash` | string | Transaction hash |
| `log_index` | uint32 | Log index in block |
| `address` | string | Contract address (redundant but useful for queries) |
| `event_signature` | string | Event topic0 (e.g. keccak256 of "Transfer(...)") |
| `topics` | []string | Indexed params (topic1, topic2, topic3) |
| `data` | string | Non-indexed data (hex) |
| `created_at` | datetime | Index time |

**Indexes:**
- `network_id` + `contract_id` + `block_number` + `log_index` (unique, for dedup)
- `network_id` (filter by network)
- `contract_id` (filter by contract)
- `block_number` (range queries)
- `transaction_hash` (lookup by tx)
- `address` + `event_signature` (filter events by contract + type)

---

## Entity Relationship

```
Network (1) ──────────< Contract (N)
    │                        │
    │                        │
    └────────────────────────┴───< Log (N)
                              (logs reference both network_id and contract_id)
```

---

## Summary

| Collection | Purpose |
|------------|---------|
| **networks** | Network config: RPCs, mainnet/testnet, active/deactive |
| **contracts** | Per-contract config: events, start_block, last_scanned_block, status, network reference |
| **logs** | Indexed event logs with network_id + contract_id for traceability |
