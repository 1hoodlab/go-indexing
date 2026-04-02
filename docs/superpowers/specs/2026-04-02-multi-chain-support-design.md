# Multi-Chain Support Design

> Index program/module events across EVM, Solana, Tron, and Sui chains using a shared interface-per-concern architecture.

## Decisions

| Decision | Choice |
|---|---|
| Goal | Index program/module events per chain |
| Deployment | Each chain = separate instance |
| Database | Shared MongoDB instance, separate database per chain |
| Data model | Each chain has its own Log model |
| Code structure | Monorepo + shared interfaces (`internal/core/`) |
| Priority | Refactor core -> Solana -> Tron -> Sui |
| Handler registry | Unified registry, string key dispatch |
| Backward compat | Refactor EVM to implement core interfaces (breaking) |
| EVM-compatible chains | Same code, different config (Ethereum, BSC, Base...) |

## 1. Core Interfaces & Types

### Generic Types (`internal/core/types.go`)

```go
// BlockEvent - chain-agnostic block notification
type BlockEvent struct {
    Number    uint64
    Hash      string
    Timestamp uint64
}

// RawEvent - chain-specific event data, unparsed
type RawEvent struct {
    BlockNumber     uint64
    TransactionHash string
    Address         string            // contract/program address
    EventKey        string            // identifier for registry dispatch
    RawData         []byte            // chain-specific raw bytes
    Metadata        map[string]string // chain-specific extra fields
}

// ContractConfig - chain-agnostic contract/program reference for pipeline use.
// Each chain's StorageInterface.LoadContracts() maps its own Contract model to this.
type ContractConfig struct {
    Address     string   // contract address / program ID / package_id
    Events      []string // event identifiers to filter
    StartBlock  uint64   // start block / slot / checkpoint
    LastScanned uint64   // last scanned block / slot / checkpoint
}
```

### Core Interfaces (`internal/core/interfaces.go`)

```go
type Subscriber interface {
    SubscribeBlocks(ctx context.Context, from uint64) (<-chan BlockEvent, error)
}

type LogFetcher interface {
    FetchLogs(ctx context.Context, block BlockEvent, contracts []ContractConfig) ([]RawEvent, error)
}

type EventParser interface {
    ParseEvent(raw RawEvent) (map[string]interface{}, error)
    EventKey(eventIdentifier string) string // convert readable name -> registry key
}
```

### Handler Registry (`internal/core/registry.go`)

```go
type Handler func(ctx context.Context, batch []RawEvent) error

type Registry struct {
    handlers       map[string]Handler  // eventKey -> handler
    defaultHandler Handler
}

func (r *Registry) Register(eventKey string, h Handler)
func (r *Registry) Dispatch(eventKey string, batch []RawEvent) error
```

Key changes from current implementation:
- Registry dispatches by `string` key instead of `[32]byte` topic0
- Handlers receive `[]RawEvent` instead of `[]*models.Log`
- Each chain decides its own `EventKey` format (EVM: keccak256 hex, Solana: program_id:event_name, Sui: event_type string)

## 2. Pipeline Refactor

### Generic Pipeline (`internal/pipeline/pipeline.go`)

The pipeline becomes chain-agnostic, receiving core interfaces via constructor:

```go
type Pipeline struct {
    subscriber  core.Subscriber
    fetcher     core.LogFetcher
    parser      core.EventParser
    registry    *core.Registry
    store       StorageInterface
    contracts   []core.ContractConfig
    blockCh     chan core.BlockEvent
}

func New(
    subscriber core.Subscriber,
    fetcher    core.LogFetcher,
    parser     core.EventParser,
    registry   *core.Registry,
    store      StorageInterface,
    contracts  []core.ContractConfig,
) *Pipeline
```

### Processing Flow

```
1. Subscriber.SubscribeBlocks() -> chan BlockEvent
2. For each block:
   a. LogFetcher.FetchLogs(block, contracts) -> []RawEvent
   b. Group RawEvents by event.EventKey
   c. Each group -> Registry.Dispatch(eventKey, batch)
3. Update last_scanned_block for contracts
```

### StorageInterface (`internal/core/storage.go`)

```go
type StorageInterface interface {
    SaveEvents(ctx context.Context, events []interface{}) error
    UpdateScanProgress(ctx context.Context, address string, block uint64) error
    LoadContracts(ctx context.Context) ([]ContractConfig, error)
    EnsureIndexes(ctx context.Context) error
}
```

Each chain implements `StorageInterface` separately because Log models differ. All use MongoDB driver underneath.

### Entry Point (`cmd/indexer/main.go`)

```go
func main() {
    cfg := config.Load()

    switch cfg.ChainType {
    case "evm":
        runEVM(cfg)
    case "solana":
        runSolana(cfg)
    case "tron":
        runTron(cfg)
    case "sui":
        runSui(cfg)
    }
}

func runEVM(cfg config.Config) {
    client := evm.NewClient(cfg.RPCURL)
    subscriber := evm.NewSubscriber(client)
    fetcher := evm.NewLogFetcher(client)
    parser := evm.NewEventParser()
    store := evm.NewStorage(cfg.MongoURI, cfg.Database)
    registry := buildEVMRegistry(store)

    p := pipeline.New(subscriber, fetcher, parser, registry, store, contracts)
    p.Start(ctx)
}
```

Key changes:
- Pipeline does not import `evm` package
- `main.go` is the only place that knows the specific chain (composition root)
- Config adds `CHAIN_TYPE` env var
- Each chain has its own `run{Chain}()` function for dependency wiring

### EVM-Compatible Chains

Ethereum, BSC, Base, Arbitrum, Polygon etc. share the same code in `internal/chains/evm/`. They differ only by config:

```bash
# Instance 1
CHAIN_TYPE=evm  CHAIN_NAME=ethereum  MONGODB_DATABASE=indexing_ethereum  RPC_URL=...  POLL_INTERVAL=12s

# Instance 2
CHAIN_TYPE=evm  CHAIN_NAME=bsc       MONGODB_DATABASE=indexing_bsc       RPC_URL=...  POLL_INTERVAL=3s

# Instance 3
CHAIN_TYPE=evm  CHAIN_NAME=base      MONGODB_DATABASE=indexing_base      RPC_URL=...  POLL_INTERVAL=2s
```

Same binary, same code, different config.

## 3. Chain-Specific Implementations

### Directory Structure

```
internal/chains/
├── evm/
│   ├── client.go          // EVM RPC client (refactored from current)
│   ├── subscriber.go      // implements core.Subscriber
│   ├── fetcher.go         // implements core.LogFetcher
│   ├── parser.go          // implements core.EventParser
│   ├── storage.go         // implements core.StorageInterface
│   └── models.go          // EVM-specific Log struct
├── solana/
│   ├── client.go          // Solana RPC client
│   ├── subscriber.go      // implements core.Subscriber (slot-based)
│   ├── fetcher.go         // implements core.LogFetcher (program logs)
│   ├── parser.go          // implements core.EventParser (Anchor IDL)
│   ├── storage.go         // implements core.StorageInterface
│   └── models.go          // Solana-specific Log struct
├── tron/
│   ├── client.go          // Tron HTTP API client
│   ├── subscriber.go
│   ├── fetcher.go
│   ├── parser.go
│   ├── storage.go
│   └── models.go
└── sui/
    ├── client.go          // Sui JSON-RPC client
    ├── subscriber.go
    ├── fetcher.go
    ├── parser.go
    ├── storage.go
    └── models.go
```

### EVM Log Model (refactored from current)

```go
type Log struct {
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

EventKey: `crypto.Keccak256Hash([]byte(eventSig)).Hex()` (same as current topic0)

### Solana Log Model

```go
type Log struct {
    Slot             uint64                 `bson:"slot"`
    BlockHash        string                 `bson:"block_hash"`
    TransactionHash  string                 `bson:"transaction_hash"`
    ProgramID        string                 `bson:"program_id"`        // Base58
    InstructionIndex uint32                 `bson:"instruction_index"`
    EventName        string                 `bson:"event_name"`
    AccountKeys      []string               `bson:"account_keys"`
    Data             string                 `bson:"data"`              // base64
    DataDecoded      map[string]interface{} `bson:"data_decoded,omitempty"`
    CreatedAt        time.Time              `bson:"created_at"`
}
```

- Subscriber polls `getSlot()`, emits `BlockEvent{Number: slot}`
- LogFetcher calls `getBlock(slot)`, filters by program IDs
- EventKey: `"program_id:event_name"` format
- EventParser uses Anchor IDL for decoding

### Tron Log Model

```go
type Log struct {
    BlockNumber     uint64                 `bson:"block_number"`
    BlockHash       string                 `bson:"block_hash"`
    TransactionHash string                 `bson:"transaction_hash"`
    LogIndex        uint32                 `bson:"log_index"`
    Address         string                 `bson:"address"`           // Base58 format
    EventSignature  string                 `bson:"event_signature"`
    Topics          []string               `bson:"topics"`
    Data            string                 `bson:"data"`
    DataDecoded     map[string]interface{} `bson:"data_decoded,omitempty"`
    CreatedAt       time.Time              `bson:"created_at"`
}
```

- Nearly identical to EVM but with Base58 addresses
- Uses Tron HTTP API (TronGrid) instead of JSON-RPC
- EventKey: Keccak256 (same as EVM, since Tron uses Solidity)

### Sui Log Model

```go
type Log struct {
    Checkpoint uint64                 `bson:"checkpoint"`
    Digest     string                 `bson:"digest"`            // transaction digest
    EventSeq   uint64                 `bson:"event_seq"`
    PackageID  string                 `bson:"package_id"`
    Module     string                 `bson:"module"`
    EventType  string                 `bson:"event_type"`        // package::module::EventStruct
    Sender     string                 `bson:"sender"`
    ParsedJSON map[string]interface{} `bson:"parsed_json"`
    CreatedAt  time.Time              `bson:"created_at"`
}
```

- Subscriber polls checkpoints
- LogFetcher uses `suix_queryEvents` API
- EventKey: `"package_id::module::EventStruct"` format

### Cross-Chain Concept Mapping

| Concept | EVM | Solana | Tron | Sui |
|---|---|---|---|---|
| Block unit | block number | slot | block number | checkpoint |
| Event source | contract address | program ID | contract address | package::module |
| Event identifier | topic0 (keccak256) | program_id:event_name | topic0 (keccak256) | event_type string |
| Address format | 0x hex (20 bytes) | Base58 (32-44 chars) | Base58Check | 0x hex (32 bytes) |
| Event data | hex encoded | base64 encoded | hex encoded | JSON |
| ABI/IDL | Solidity ABI JSON | Anchor IDL JSON | Solidity ABI JSON | Move ABI |

## 4. Configuration & Seeding

### Config (`internal/config/config.go`)

```go
type Config struct {
    ChainType    string        // "evm", "solana", "tron", "sui"
    ChainName    string        // "ethereum", "bsc", "solana-mainnet"...
    RPCURL       string
    PollInterval time.Duration
    MongoURI     string
    Database     string        // "indexing_ethereum", "indexing_solana"...
}
```

Environment variables:

```bash
CHAIN_TYPE=evm
CHAIN_NAME=ethereum
RPC_URL=https://eth-mainnet.g.alchemy.com/v2/xxx
POLL_INTERVAL=12s
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=indexing_ethereum
```

Default poll intervals (when `POLL_INTERVAL` is not set):

| Chain Type | Default |
|---|---|
| evm | 12s |
| solana | 400ms |
| tron | 3s |
| sui | 1s |

### Seeding (`cmd/seed/`)

Refactored from hardcoded Sepolia to config-driven JSON files:

```
cmd/seed/
├── main.go              // reads chain type, calls appropriate seeder
└── data/
    ├── ethereum.json
    ├── bsc.json
    ├── solana.json
    ├── tron.json
    └── sui.json
```

Seed file format example (`solana.json`):

```json
{
  "network": {
    "name": "Solana Mainnet",
    "chain_type": "solana",
    "rpc_urls": ["https://api.mainnet-beta.solana.com"],
    "type": "mainnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "events": ["Transfer", "Approve"],
      "idl": "token_program_idl.json",
      "start_block": 250000000
    }
  ]
}
```

Usage: `go run cmd/seed/main.go --config data/solana.json`

### Network Model (extended)

```go
type Network struct {
    ID        primitive.ObjectID `bson:"_id,omitempty"`
    Name      string             `bson:"name"`
    ChainType string             `bson:"chain_type"`   // NEW: "evm", "solana", "tron", "sui"
    ChainID   int64              `bson:"chain_id"`      // EVM chains, 0 for non-EVM
    Type      NetworkType        `bson:"type"`
    RPCURLs   []string           `bson:"rpc_urls"`
    Status    NetworkStatus      `bson:"status"`
    CreatedAt time.Time          `bson:"created_at"`
    UpdatedAt time.Time          `bson:"updated_at"`
}
```

### Contract Models (per chain)

Each chain has its own Contract model in its package, with only the fields relevant to that chain:

```go
// chains/evm/models.go
type Contract struct {
    ID               primitive.ObjectID `bson:"_id,omitempty"`
    NetworkID        primitive.ObjectID `bson:"network_id"`
    Address          string             `bson:"address"`           // 0x hex
    ABI              string             `bson:"abi"`               // Solidity ABI JSON
    Events           []string           `bson:"events"`
    StartBlock       uint64             `bson:"start_block"`
    LastScannedBlock uint64             `bson:"last_scanned_block"`
    Status           NetworkStatus      `bson:"status"`
    CreatedAt        time.Time          `bson:"created_at"`
    UpdatedAt        time.Time          `bson:"updated_at"`
}

// chains/solana/models.go
type Contract struct {
    ID               primitive.ObjectID `bson:"_id,omitempty"`
    NetworkID        primitive.ObjectID `bson:"network_id"`
    Address          string             `bson:"address"`           // Base58 program ID
    IDL              string             `bson:"idl"`               // Anchor IDL JSON
    Events           []string           `bson:"events"`
    StartSlot        uint64             `bson:"start_slot"`
    LastScannedSlot  uint64             `bson:"last_scanned_slot"`
    Status           NetworkStatus      `bson:"status"`
    CreatedAt        time.Time          `bson:"created_at"`
    UpdatedAt        time.Time          `bson:"updated_at"`
}

// chains/tron/models.go
type Contract struct {
    ID               primitive.ObjectID `bson:"_id,omitempty"`
    NetworkID        primitive.ObjectID `bson:"network_id"`
    Address          string             `bson:"address"`           // Base58Check
    ABI              string             `bson:"abi"`               // Solidity ABI JSON
    Events           []string           `bson:"events"`
    StartBlock       uint64             `bson:"start_block"`
    LastScannedBlock uint64             `bson:"last_scanned_block"`
    Status           NetworkStatus      `bson:"status"`
    CreatedAt        time.Time          `bson:"created_at"`
    UpdatedAt        time.Time          `bson:"updated_at"`
}

// chains/sui/models.go
type Contract struct {
    ID                   primitive.ObjectID `bson:"_id,omitempty"`
    NetworkID            primitive.ObjectID `bson:"network_id"`
    PackageID            string             `bson:"package_id"`        // 0x hex
    Module               string             `bson:"module"`
    MoveABI              string             `bson:"move_abi"`          // Move ABI
    Events               []string           `bson:"events"`
    StartCheckpoint      uint64             `bson:"start_checkpoint"`
    LastScannedCheckpoint uint64            `bson:"last_scanned_checkpoint"`
    Status               NetworkStatus      `bson:"status"`
    CreatedAt            time.Time          `bson:"created_at"`
    UpdatedAt            time.Time          `bson:"updated_at"`
}
```

This approach lets each chain use domain-specific field names (`start_slot` vs `start_block` vs `start_checkpoint`, `address` vs `package_id`) instead of forcing generic terminology.

## 5. Error Handling, Resilience & Testing

### Error Types (`internal/core/errors.go`)

```go
var (
    ErrRPCTimeout    = errors.New("rpc timeout")
    ErrRPCRateLimit  = errors.New("rpc rate limited")
    ErrBlockNotFound = errors.New("block not found")
    ErrParseEvent    = errors.New("event parse failed")
)
```

### Retry Strategy

| Error | Behavior |
|---|---|
| RPC timeout / rate limit | Retry with exponential backoff (max 5 attempts) |
| Block not found | Skip, log warning (chain reorg or not finalized) |
| Event parse failed | Skip event, log error, continue block |
| MongoDB write failed | Retry 3 times, stop pipeline if still failing |
| Duplicate key | Ignore (keep current behavior, idempotent) |

### Graceful Shutdown

Same pattern as current implementation - context cancellation propagates through all layers:

```
SIGINT/SIGTERM -> cancel context -> Subscriber stops polling
                                 -> Pipeline drains blockCh
                                 -> Flush pending writes
                                 -> Close MongoDB connection
```

### Testing Strategy

Three tiers since each chain implements core interfaces:

**1. Unit tests - mock core interfaces**

```go
// pipeline/pipeline_test.go
func TestPipeline_GroupsEventsByKey(t *testing.T) {
    mockSub := &MockSubscriber{blocks: []core.BlockEvent{{Number: 100}}}
    mockFetcher := &MockLogFetcher{events: []core.RawEvent{...}}
    mockParser := &MockEventParser{}
    // assert correct grouping and dispatch
}
```

**2. Chain-specific unit tests**

```go
// chains/evm/parser_test.go - test EVM event parsing
// chains/solana/fetcher_test.go - test Solana log extraction
```

**3. Integration tests (optional, skipped in CI by default)**

```go
//go:build integration

// chains/evm/integration_test.go
// Test against real testnet RPC (Sepolia, Solana Devnet...)
```

## Implementation Priority

1. **Phase 1:** Create `internal/core/` with interfaces and types, refactor EVM to implement them, refactor pipeline to be chain-agnostic
2. **Phase 2:** Implement Solana chain adapter
3. **Phase 3:** Implement Tron chain adapter
4. **Phase 4:** Implement Sui chain adapter
