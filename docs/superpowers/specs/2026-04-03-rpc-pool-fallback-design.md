# RPC Pool Fallback Design

> RPCPool wrapper that provides automatic fallback across multiple RPC URLs loaded from the database.

## Decisions

| Decision | Choice |
|---|---|
| When to rotate | Immediately on any RPC error |
| When all URLs fail | Return error to caller |
| Reuse failed URLs | No — failed URLs are removed, restart to reset |
| Approach | RPCPool middleware wrapping Client |
| RPC URL source | Database (network.rpc_urls), not env var |

## 1. RPCPool Struct

```go
// internal/chains/evm/rpcpool.go

type RPCClient interface {
    HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
    BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
    FilterLogs(ctx context.Context, from, to *big.Int, addresses []common.Address) ([]types.Log, error)
}

type RPCPool struct {
    mu        sync.RWMutex
    active    RPCClient
    activeURL string
    remaining []string
    dialFn    func(url string) (RPCClient, error)
}
```

- `active`: current connected client
- `remaining`: URLs not yet tried, popped on rotate
- `dialFn`: injected for testing (production: wraps `NewClient`)

### Errors

```go
var (
    ErrNoRPCURLs     = errors.New("no RPC URLs provided")
    ErrAllRPCsFailed = errors.New("all RPC URLs failed")
)
```

## 2. Lifecycle

### Construction

```
NewRPCPool(urls []string) (*RPCPool, error)
  - urls empty → ErrNoRPCURLs
  - dialFn(urls[0]) → active client
  - remaining = urls[1:]
  - If urls[0] fails, try urls[1], urls[2]... until one connects
  - If all fail at construction → return error
```

### Call Flow

```
pool.HeaderByNumber(ctx, number)
  → RLock, get active client
  → call active.HeaderByNumber()
  → success → return result
  → fail → rotate()
    → Lock
    → pop remaining[0], dialFn(url) → new active
    → retry call with new active
    → if remaining empty → return ErrAllRPCsFailed
```

### Rotate

```
rotate() (*RPCClient, error)
  - Loop through remaining:
    - Pop first URL
    - dialFn(url) → success → set as active, return
    - fail → log, continue to next
  - remaining empty → return ErrAllRPCsFailed
```

Failed URLs are not put back. Once removed, they stay removed until process restart.

## 3. Methods

Same signatures as `Client`, so consumers can swap transparently:

```go
func (p *RPCPool) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
func (p *RPCPool) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
func (p *RPCPool) FilterLogs(ctx context.Context, from, to *big.Int, addresses []common.Address) ([]types.Log, error)
func (p *RPCPool) ActiveURL() string
```

Thread safety: `RWMutex` — read lock for calls, write lock for rotate.

## 4. Consumer Changes

### Storage — add LoadNetwork

```go
// internal/chains/evm/storage.go
func (s *Storage) LoadNetwork(ctx context.Context) (*models.Network, error)
```

Loads first active network from DB. Returns network with `RPCURLs` field.

### Subscriber and LogFetcher

Change `*Client` parameter to `RPCClient` interface:

```go
// Before
func NewSubscriber(client *Client, pollInterval time.Duration) *Subscriber
func NewLogFetcher(client *Client, networkID string, contracts []*Contract) *LogFetcher

// After
func NewSubscriber(client RPCClient, pollInterval time.Duration) *Subscriber
func NewLogFetcher(client RPCClient, networkID string, contracts []*Contract) *LogFetcher
```

Internal fields change from `*Client` to `RPCClient`.

### Entry Point

```go
// cmd/indexer/main.go — runEVM()
network, err := store.LoadNetwork(ctx)
pool, err := evm.NewRPCPool(network.RPCURLs)

subscriber := evm.NewSubscriber(pool, cfg.PollInterval)
fetcher := evm.NewLogFetcher(pool, network.ID.Hex(), contractModels)
```

### Config

`RPC_URL` env var removed entirely (already done). RPC URLs come only from DB.

### README

Remove `RPC_URL` from env var table. Add note that RPC URLs are configured via seed data in DB.

## 5. Testing

```go
// internal/chains/evm/rpcpool_test.go

// TestRPCPool_UsesFirstURL
// Create pool with mock dialFn, verify first URL is used

// TestRPCPool_RotatesOnFailure
// First client fails → verify pool switches to second URL and retries

// TestRPCPool_ReturnsErrorWhenAllFail
// All URLs fail → verify ErrAllRPCsFailed returned

// TestRPCPool_FailedURLNotReused
// URL 1 fails, rotate to URL 2, URL 2 fails → verify does NOT retry URL 1
```

Mock via `dialFn` injection — no real RPC connections needed.

## Files Changed

| File | Change |
|---|---|
| Create: `internal/chains/evm/rpcpool.go` | RPCPool, RPCClient interface, errors |
| Create: `internal/chains/evm/rpcpool_test.go` | 4 unit tests |
| Modify: `internal/chains/evm/subscriber.go` | `*Client` → `RPCClient` |
| Modify: `internal/chains/evm/fetcher.go` | `*Client` → `RPCClient` |
| Modify: `internal/chains/evm/storage.go` | Add `LoadNetwork()` |
| Modify: `cmd/indexer/main.go` | Load network from DB, use RPCPool |
| Modify: `README.md` | Remove RPC_URL, add note about DB config |
