# Decoded Events Collection Design

> Automatically decode all indexed events via ABI and store structured data in a `decoded_events` collection.

## Decisions

| Decision | Choice |
|---|---|
| Storage | New `decoded_events` collection |
| Structure | Common fields + `data` map for decoded params |
| Which events | All events automatically (no custom handlers needed) |
| Decode failure | Skip decoded_events, raw log still saved, log warning |
| Custom handlers | Remove MemberAdded — default handler does everything |

## 1. DecodedEvent Model

```go
// internal/chains/evm/models.go
type DecodedEvent struct {
    ID              primitive.ObjectID     `bson:"_id,omitempty"`
    NetworkID       primitive.ObjectID     `bson:"network_id"`
    ContractAddress string                 `bson:"contract_address"`
    EventName       string                 `bson:"event_name"`
    BlockNumber     uint64                 `bson:"block_number"`
    TransactionHash string                 `bson:"transaction_hash"`
    LogIndex        uint32                 `bson:"log_index"`
    Data            map[string]interface{} `bson:"data"`
    CreatedAt       time.Time              `bson:"created_at"`
}
```

Example document:

```json
{
  "network_id": "ObjectId(...)",
  "contract_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
  "event_name": "Transfer",
  "block_number": 23980100,
  "transaction_hash": "0xabc...",
  "log_index": 5,
  "data": { "from": "0x...", "to": "0x...", "value": "1000000" },
  "created_at": "2026-04-03T..."
}
```

## 2. Storage Changes

### core.StorageInterface — add method

```go
type StorageInterface interface {
    // ... existing
    SaveDecodedEvents(ctx context.Context, events []interface{}) error
}
```

### EVM Storage

```go
func (s *Storage) DecodedEvents() *mongo.Collection
func (s *Storage) SaveDecodedEvents(ctx context.Context, events []interface{}) error
```

Bulk insert with `SetOrdered(false)`, skip duplicates.

### Index

```
(contract_address, event_name, block_number, log_index) — unique
```

## 3. Default Handler — Auto Decode

```
RawEvent → Default Handler:
  1. Save raw log to `logs` collection (unchanged)
  2. Find event name from ABI by matching topic0
  3. Decode params via utils.DecodeEventLog
  4. Save to `decoded_events` collection
  5. Decode fail → skip decoded_events, log warning
```

### Helper: FindEventNameByTopic0

```go
// pkg/utils/abi_event.go
func FindEventNameByTopic0(abiJSON string, topic0 string) string
```

Parses ABI JSON, computes keccak256 of each event's canonical signature, returns event name matching the given topic0. Returns "" if no match.

## 4. Remove MemberAdded Custom Handler

- Delete `internal/handlers/member_added.go`
- Remove handler registration from `cmd/indexer/main.go`
- Default handler now handles all events automatically

## Files Changed

| File | Change |
|---|---|
| Modify: `internal/core/interfaces.go` | Add `SaveDecodedEvents` to StorageInterface |
| Modify: `internal/chains/evm/models.go` | Add `DecodedEvent` struct |
| Modify: `internal/chains/evm/storage.go` | Add `DecodedEvents()`, `SaveDecodedEvents()`, index |
| Modify: `internal/handlers/default.go` | Auto decode + save decoded_events |
| Create: `pkg/utils/abi_event.go` | `FindEventNameByTopic0()` |
| Delete: `internal/handlers/member_added.go` | No longer needed |
| Modify: `cmd/indexer/main.go` | Remove MemberAdded registration |
| Modify: `internal/pipeline/pipeline_test.go` | Mock `SaveDecodedEvents` |
