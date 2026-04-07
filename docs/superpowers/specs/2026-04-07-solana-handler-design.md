# Solana Network Handler Design

**Date:** 2026-04-07  
**Status:** Approved  

## Overview

Design for Solana network support in go-indexing. Implements the existing `core.Subscriber`, `core.LogFetcher`, `core.EventParser`, and `core.StorageInterface` interfaces in a new `internal/chains/solana/` package. The pipeline and handler layers remain unchanged.

## Key Decisions

| Decision | Choice | Reason |
|---|---|---|
| Live tracking | HTTP polling (`getSlot` + `getBlocksWithLimit`) | `blockSubscribe` WS is unstable/unavailable on most nodes |
| Poll interval | 400ms (config default) | Matches Solana slot time |
| Commitment | `confirmed` | ~800ms lag, negligible reorg risk on mainnet |
| Event extraction | `meta.logMessages` → `"Program data: <base64>"` | Idiomatic Anchor event pattern |
| Decoding | Anchor IDL discriminator + borsh; raw fallback | Support both IDL and unknown programs |
| EventKey | `programAddress:discriminatorHex` | Unique, human-readable, no hash needed |
| Go client | `github.com/gagliardetto/solana-go` | De-facto standard, built-in borsh + IDL support |

## Concept Mapping: EVM → Solana

| Concept | EVM | Solana |
|---|---|---|
| Block unit | `block_number` | `slot` |
| Address format | `0x` hex | Base58 pubkey |
| Contract | `address` | `program_id` |
| EventKey | `topic0` (keccak256) | `programID:discriminatorHex` |
| Event data | `log.Data` hex | `"Program data: <base64>"` log line |
| ABI | Solidity JSON ABI | Anchor IDL JSON |
| Config collection | `contracts` | `programs` |
| Log collection | `logs` | `program_logs` |

## File Structure

```
internal/chains/solana/
├── client.go        # Wraps gagliardetto/solana-go RPC client
├── rpcpool.go       # Multi-URL fallback (same pattern as evm/rpcpool.go)
├── subscriber.go    # Implements core.Subscriber
├── fetcher.go       # Implements core.LogFetcher
├── parser.go        # Implements core.EventParser
├── storage.go       # Implements core.StorageInterface
├── models.go        # Solana-specific MongoDB models
└── *_test.go
```

## Data Flow

```
Subscriber (poll getSlot every 400ms)
    ↓ chan BlockEvent{Number: slot, Hash: blockhash, Timestamp: blockTime}
Pipeline (unchanged)
    ↓
Fetcher → getBlock(slot, confirmed, maxSupportedTransactionVersion=0)
    → filter txs: program_id in (accountKeys ∪ loadedAddresses)
    → parse logMessages → "Program data: <base64>" lines (scoped to program invocation)
    → emit RawEvent per event line
    ↓ []RawEvent grouped by EventKey
Registry → Handler (unchanged)
    ↓
Storage → MongoDB (solana schema)
```

## Component Designs

### Subscriber (`subscriber.go`)

Polls chain tip and emits confirmed slots that contain blocks.

```
loop every 400ms:
  currentSlot = getSlot(commitment: confirmed)
  if currentSlot <= lastProcessed: continue

  confirmedSlots = getBlocksWithLimit(lastProcessed+1, limit=100, commitment: confirmed)
  for each slot in confirmedSlots:
    send BlockEvent{Number: slot, Hash: ..., Timestamp: ...} → out channel

  lastProcessed = confirmedSlots.last
```

`getBlocksWithLimit` is used instead of iterating slots directly because not every slot produces a block (leader skips). This avoids calling `getBlock` on empty slots.

On startup, `from` is set to the minimum `last_scanned_slot` across all watched programs (backfill).

### Fetcher (`fetcher.go`)

**Block fetch parameters:**
```json
{
  "commitment": "confirmed",
  "encoding": "base64",
  "transactionDetails": "full",
  "maxSupportedTransactionVersion": 0,
  "rewards": false
}
```

`maxSupportedTransactionVersion: 0` is required to receive versioned transactions (Address Lookup Tables).

**Transaction filtering:**

For each transaction, build the full account set:
```
allAccounts = message.accountKeys
           ∪ meta.loadedAddresses.writable
           ∪ meta.loadedAddresses.readonly
```

A transaction matches if any watched `program_id` appears in `allAccounts`.

**Log extraction — scoped to program invocation:**

Track invocation depth per program by parsing log markers:
- `"Program <addr> invoke [N]"` → push scope for `<addr>` at depth N
- `"Program <addr> success"` / `"Program <addr> failed"` → pop scope
- `"Program data: <base64>"` → emit event only if currently inside target program's scope

This prevents capturing events emitted by other programs in the same transaction.

**`RawEvent` output:**
```go
RawEvent{
    BlockNumber:     slot,
    TransactionHash: tx.Signatures[0].String(),  // base58
    Address:         programAddress,              // base58 pubkey
    EventKey:        programAddress + ":" + discriminatorHex,  // first 8 bytes
    RawData:         decodedBase64Bytes,
    Metadata: map[string]string{
        "idl":          contractConfig.IDL,        // Anchor IDL JSON, empty if none
        "slot":         strconv.FormatUint(slot, 10),
        "log_index":    strconv.Itoa(i),
        "account_keys": jsonArrayOfAllAccounts,
    },
}
```

**Idempotency key:** `(network_id, program_id, slot, transaction_hash, log_index)` — unique per event emission.

### Parser (`parser.go`)

**Anchor IDL path (when IDL available):**

Anchor events are prefixed with an 8-byte discriminator: `sha256("event:<EventName>")[0:8]`.

```
RawData[0:8]  → discriminator → lookup event in IDL → event name + field schema
RawData[8:]   → borsh decode with field schema → map[string]interface{}
```

Uses `gagliardetto/solana-go`'s built-in borsh package for decoding.

Supported borsh types: `u8`, `u16`, `u32`, `u64`, `u128`, `i64`, `bool`, `publicKey` (32 bytes → Base58 string), `string`, `vec<T>`, `option<T>`, nested structs.

**Raw fallback (no IDL or unknown discriminator):**
```go
return map[string]interface{}{
    "discriminator": hex.EncodeToString(raw.RawData[:8]),
    "data_base64":   base64.StdEncoding.EncodeToString(raw.RawData),
    "data_hex":      hex.EncodeToString(raw.RawData),
}, nil
```

**`EventKey` method:**
```go
func (p *Parser) EventKey(identifier string) string {
    return identifier  // already canonical: "programAddress:discriminatorHex"
}
```

### Storage (`storage.go`)

Implements `core.StorageInterface`. Uses three MongoDB collections.

**`programs` collection** (equivalent to EVM `contracts`):
```go
type Program struct {
    ID               primitive.ObjectID
    NetworkID        primitive.ObjectID
    Address          string       // base58 program pubkey
    IDL              string       // Anchor IDL JSON (optional)
    Events           []string     // event names to watch
    StartSlot        uint64
    LastScannedSlot  uint64
    Status           string       // "active" | "deactive"
    CreatedAt        time.Time
    UpdatedAt        time.Time
}
```

**`program_logs` collection** (equivalent to EVM `logs`):
```go
type ProgramLog struct {
    ID              primitive.ObjectID
    NetworkID       primitive.ObjectID
    ProgramID       primitive.ObjectID
    Slot            uint64
    BlockHash       string
    TransactionHash string       // base58 signature
    LogIndex        int
    ProgramAddress  string       // base58 pubkey
    Discriminator   string       // hex, first 8 bytes
    EventKey        string       // "programAddress:discriminatorHex"
    RawData         string       // base64 original bytes
    AccountKeys     []string     // all accounts involved in tx
    CreatedAt       time.Time
}
```

**`decoded_events` collection:**
```go
type DecodedEvent struct {
    ID              primitive.ObjectID
    NetworkID       primitive.ObjectID
    ProgramAddress  string
    EventName       string       // from IDL, or discriminatorHex if no IDL
    Slot            uint64
    TransactionHash string
    LogIndex        int
    Data            map[string]interface{}
    CreatedAt       time.Time
}
```

**MongoDB indexes:**
- `program_logs`: unique on `(network_id, program_id, slot, transaction_hash, log_index)`
- `program_logs`: compound on `(program_address, discriminator)`
- `decoded_events`: unique on `(program_address, event_name, slot, transaction_hash, log_index)`

**Interface method mapping:**
- `SaveEvents()` → insert into `program_logs`
- `SaveDecodedEvents()` → insert into `decoded_events`
- `UpdateScanProgress(address, slot)` → update `programs.last_scanned_slot`
- `LoadContracts()` → load from `programs`, map to `ContractConfig{Address, Events, StartBlock: StartSlot, LastScanned: LastScannedSlot}`

### RPC Pool (`rpcpool.go`)

Same pattern as `evm/rpcpool.go`:
- Holds list of `*rpc.Client` from `gagliardetto/solana-go`
- `callWithFallback[T]` generic helper: try current URL, on error rotate to next
- Failed URLs permanently removed from pool (restart to reset)
- Thread-safe with mutex

### Config & Wiring

**Environment variables** (no new vars needed):
```bash
CHAIN_TYPE=solana
CHAIN_NAME=solana-mainnet
POLL_INTERVAL=400ms
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=indexing_solana
```

**`cmd/indexer/main.go`** — add case:
```go
switch cfg.ChainType {
case "evm":
    runEVM(cfg)
case "solana":
    runSolana(cfg)
}
```

**`runSolana(cfg)`** — composition root:
```go
func runSolana(cfg *config.Config) {
    pool     := solana.NewRPCPool(network.RPCURLs)
    store    := solana.NewStorage(mongoClient, cfg.Database)
    sub      := solana.NewSubscriber(pool, store)
    fetcher  := solana.NewFetcher(pool, contracts)
    parser   := solana.NewParser()
    registry := core.NewRegistry()
    registry.RegisterDefault(handlers.NewDefaultHandler(store, parser))
    pipeline := pipeline.New(sub, fetcher, registry, store)
    pipeline.Run(ctx)
}
```

**Seed file format** (`cmd/seed/data/solana-mainnet.json`):
```json
{
  "network": {
    "name": "Solana Mainnet",
    "chain_type": "solana",
    "chain_id": 0,
    "rpc_urls": ["https://your-dedicated-rpc.com"],
    "type": "mainnet",
    "status": "active"
  },
  "contracts": [
    {
      "address": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "events": ["Transfer", "Approve"],
      "idl": "{...anchor idl json...}",
      "start_block": 250000000
    }
  ]
}
```

The existing `cmd/seed/main.go` already handles the `idl` field — no changes needed.

## Error Handling

- `getBlock` returns `null` for skipped/empty slots → skip silently (not an error; handled upstream by `getBlocksWithLimit`)
- Transaction decode errors → log warning, skip transaction, continue
- Borsh decode failure → fall back to raw output (never drop the event)
- RPC errors → rotate to next URL via pool; if all URLs exhausted → return error, pipeline retries
- Duplicate inserts (reprocessing) → caught by unique indexes, ignored (same as EVM)

## Testing

- `subscriber_test.go`: mock RPC returning predefined slot sequences including gaps; verify correct slots emitted and backfill behavior
- `fetcher_test.go`: fixture block JSON with known `logMessages`; verify correct `RawEvent` extraction including scope tracking
- `parser_test.go`: known discriminator + borsh-encoded bytes with IDL; verify decoded fields; verify raw fallback for unknown discriminator
- `storage_test.go`: mock MongoDB; verify correct collection writes and index upsert behavior
- `rpcpool_test.go`: verify URL rotation on error; verify permanent removal after repeated failure
