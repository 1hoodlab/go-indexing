# Go Indexing

Multi-chain blockchain event indexer. Indexes program/module events from EVM (Ethereum, BSC, Base...), with Solana, Tron, and Sui support planned.

## Prerequisites

- Go 1.25+
- Docker (for MongoDB)

## Quick Start

### 1. Start MongoDB

```bash
docker compose up -d
```

### 2. Seed the database

```bash
go run cmd/seed/main.go --config cmd/seed/data/sepolia.json
```

This creates the network and contract entries in MongoDB for Sepolia testnet.

### 3. Run the indexer

```bash
CHAIN_TYPE=evm \
CHAIN_NAME=sepolia \
MONGODB_DATABASE=indexing_sepolia \
go run cmd/indexer/main.go
```

> **Note:** RPC URLs are loaded from the database (`network.rpc_urls`), not from environment variables. Configure them in your seed data JSON files.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CHAIN_TYPE` | `evm` | Chain type: `evm`, `solana`, `tron`, `sui` |
| `CHAIN_NAME` | same as CHAIN_TYPE | Display name for logs |
| `POLL_INTERVAL` | per chain type | Block polling interval (e.g. `12s`, `400ms`) |
| `MONGODB_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGODB_DATABASE` | `indexing` | Database name |

> **Note:** RPC URLs are configured in seed data / database (`network.rpc_urls`), not via environment variables. The indexer uses an `RPCPool` that automatically falls back to the next URL on failure.

### Default Poll Intervals

| Chain | Interval |
|---|---|
| EVM | 12s |
| Solana | 400ms |
| Tron | 3s |
| Sui | 1s |

## Running Multiple EVM Chains

Same binary, different config per instance:

```bash
# Ethereum Mainnet
CHAIN_TYPE=evm CHAIN_NAME=ethereum MONGODB_DATABASE=indexing_ethereum go run cmd/indexer/main.go

# BSC
CHAIN_TYPE=evm CHAIN_NAME=bsc MONGODB_DATABASE=indexing_bsc POLL_INTERVAL=3s go run cmd/indexer/main.go

# Base
CHAIN_TYPE=evm CHAIN_NAME=base MONGODB_DATABASE=indexing_base POLL_INTERVAL=2s go run cmd/indexer/main.go
```

## Seeding New Chains

Create a JSON file in `cmd/seed/data/`:

```json
{
  "network": {
    "name": "Ethereum Mainnet",
    "chain_type": "evm",
    "chain_id": 1,
    "rpc_urls": ["https://eth-rpc.example.com"],
    "type": "mainnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "0x...",
      "abi": [...],
      "events": ["Transfer(address,address,uint256)"],
      "start_block": 12345678
    }
  ]
}
```

Then run:

```bash
MONGODB_DATABASE=indexing_ethereum go run cmd/seed/main.go --config cmd/seed/data/ethereum.json
```

## Project Structure

```
cmd/
  indexer/          # Main indexer entry point (composition root)
  seed/             # Database seeding utility
    data/           # Seed JSON files per chain
internal/
  core/             # Chain-agnostic interfaces and types
  chains/
    evm/            # EVM implementation (Ethereum, BSC, Base...)
  config/           # Environment-based configuration
  handlers/         # Event handlers (default + custom)
  models/           # Shared models (Network)
  pipeline/         # Chain-agnostic indexing pipeline
  storage/          # Generic MongoDB wrapper
pkg/utils/          # ABI decoding, signature hashing
```

## Tests

```bash
go test ./... -v
```
