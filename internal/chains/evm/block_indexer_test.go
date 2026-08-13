package evm

import (
	"context"
	"errors"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
)

// fakeChainRPC simulates a chain of headers and records which ranges/blocks were requested.
type fakeChainRPC struct {
	headers     map[uint64]*types.Header
	head        uint64
	filterCalls [][2]uint64
	headerReqs  []uint64
}

func newFakeChain(from, to uint64) *fakeChainRPC {
	f := &fakeChainRPC{headers: make(map[uint64]*types.Header), head: to}
	parent := common.Hash{}
	for n := from; n <= to; n++ {
		h := &types.Header{
			Number:     new(big.Int).SetUint64(n),
			ParentHash: parent,
			Time:       1700000000 + n,
		}
		f.headers[n] = h
		parent = h.Hash()
	}
	return f
}

func (f *fakeChainRPC) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if number == nil {
		return f.headers[f.head], nil
	}
	n := number.Uint64()
	f.headerReqs = append(f.headerReqs, n)
	h, ok := f.headers[n]
	if !ok {
		return nil, errors.New("header not found")
	}
	return h, nil
}

func (f *fakeChainRPC) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeChainRPC) FilterLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []common.Address) ([]types.Log, error) {
	f.filterCalls = append(f.filterCalls, [2]uint64{fromBlock.Uint64(), toBlock.Uint64()})
	return nil, nil
}

func (f *fakeChainRPC) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error) {
	return []RpcReceipt{}, nil
}

type persistCall struct {
	block          postgres.BlockRow
	nextCheckpoint uint64
}

// fakeIndexerStore records persistence calls in memory.
type fakeIndexerStore struct {
	runtime        *postgres.IndexerRuntimeConfig
	blocks         map[uint64]*postgres.BlockRow
	catchupCPs     []uint64
	persistCalls   []persistCall
	deletedHashes  []string
	setCheckpoints []uint64
}

func (s *fakeIndexerStore) LoadIndexerRuntimeConfig(ctx context.Context) (*postgres.IndexerRuntimeConfig, error) {
	return s.runtime, nil
}

func (s *fakeIndexerStore) ConfirmPendingBlocks(ctx context.Context, networkID int64, confirmedHead uint64) (int, error) {
	return 0, nil
}

func (s *fakeIndexerStore) PersistBlock(ctx context.Context, networkID, chainID int64, block postgres.BlockRow, logs []postgres.LogInsert, enqueueOutbox bool, nextCheckpoint uint64) (*postgres.PersistBlockResult, error) {
	s.persistCalls = append(s.persistCalls, persistCall{block: block, nextCheckpoint: nextCheckpoint})
	return &postgres.PersistBlockResult{BlockID: 1}, nil
}

func (s *fakeIndexerStore) PersistCatchupBatch(ctx context.Context, networkID, chainID int64, logs []postgres.LogInsert, enqueueOutbox bool, nextCheckpoint uint64) ([]int64, error) {
	s.catchupCPs = append(s.catchupCPs, nextCheckpoint)
	return nil, nil
}

func (s *fakeIndexerStore) GetBlockByNumber(ctx context.Context, networkID int64, blockNumber uint64) (*postgres.BlockRow, error) {
	if b, ok := s.blocks[blockNumber]; ok {
		return b, nil
	}
	return nil, errors.New("no rows")
}

func (s *fakeIndexerStore) DeleteBlockByHash(ctx context.Context, networkID int64, blockHash string) error {
	s.deletedHashes = append(s.deletedHashes, blockHash)
	return nil
}

func (s *fakeIndexerStore) SetCheckpoint(ctx context.Context, networkID int64, block uint64) error {
	s.setCheckpoints = append(s.setCheckpoints, block)
	return nil
}

func newTestIndexer(t *testing.T, batchSize uint64, checkpoint uint64, chain *fakeChainRPC) (*BlockIndexer, *fakeIndexerStore) {
	t.Helper()
	runtime := &postgres.IndexerRuntimeConfig{
		NetworkID:         1,
		ChainID:           31337,
		Checkpoint:        checkpoint,
		ScanEnabled:       true,
		ConfirmationDepth: 2,
		ContractEvents: []postgres.ContractEvent{
			{ContractID: 1, Address: "0x00000000000000000000000000000000000000aa"},
		},
		Addresses: []string{"0x00000000000000000000000000000000000000aa"},
	}
	store := &fakeIndexerStore{runtime: runtime, blocks: make(map[uint64]*postgres.BlockRow)}
	pool, err := NewRPCPoolWithDialFn([]string{"fake"}, func(url string) (RPCClient, error) {
		return chain, nil
	})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	cfg := config.Config{BlockBatchSize: batchSize}
	return NewBlockIndexer(cfg, store, runtime, pool), store
}

// Checkpoint semantics: the stored checkpoint is the NEXT block to scan
// (seed-pg stores min(start_block); SetCheckpoint describes it the same way).
// Catch-up must therefore begin at the checkpoint itself.
func TestProcessCatchup_ScansFromCheckpoint(t *testing.T) {
	chain := newFakeChain(90, 160)
	idx, store := newTestIndexer(t, 100, 100, chain)

	if err := idx.processCatchup(context.Background(), 100, 150); err != nil {
		t.Fatalf("processCatchup: %v", err)
	}

	if len(chain.filterCalls) == 0 {
		t.Fatal("expected at least one getLogs call")
	}
	if got := chain.filterCalls[0]; got != [2]uint64{100, 150} {
		t.Errorf("first getLogs range = %d-%d, want 100-150 (block at checkpoint must be scanned)", got[0], got[1])
	}
	if len(store.catchupCPs) == 0 || store.catchupCPs[len(store.catchupCPs)-1] != 151 {
		t.Errorf("final checkpoint = %v, want 151 (next block to scan)", store.catchupCPs)
	}
}

// After a batch finishes and the indexer resumes from the stored checkpoint,
// no block may be skipped and none double-scanned.
func TestProcessCatchup_ResumeFromStoredCheckpointHasNoGap(t *testing.T) {
	chain := newFakeChain(90, 170)
	idx, store := newTestIndexer(t, 25, 100, chain)

	if err := idx.processCatchup(context.Background(), 100, 150); err != nil {
		t.Fatalf("processCatchup: %v", err)
	}
	if len(store.catchupCPs) == 0 {
		t.Fatal("no checkpoint recorded")
	}
	resumeCP := store.catchupCPs[len(store.catchupCPs)-1]

	// Simulate restart: a fresh catch-up starting from the persisted checkpoint.
	if err := idx.processCatchup(context.Background(), resumeCP, 160); err != nil {
		t.Fatalf("processCatchup resume: %v", err)
	}

	next := uint64(100)
	for _, r := range chain.filterCalls {
		if r[0] != next {
			t.Fatalf("getLogs ranges %v: range starts at %d, want %d (gap or overlap)", chain.filterCalls, r[0], next)
		}
		next = r[1] + 1
	}
	if next != 161 {
		t.Fatalf("ranges %v end at %d, want coverage through 160", chain.filterCalls, next-1)
	}
}

// In tip mode the block at the checkpoint itself must be indexed,
// and the stored checkpoint advances to the block after it.
func TestProcessTip_IndexesBlockAtCheckpoint(t *testing.T) {
	chain := newFakeChain(190, 205)
	idx, store := newTestIndexer(t, 100, 200, chain)
	store.blocks[199] = &postgres.BlockRow{
		BlockNumber: 199,
		BlockHash:   chain.headers[199].Hash().Hex(),
	}

	if err := idx.processTip(context.Background(), 200, chain.headers[205]); err != nil {
		t.Fatalf("processTip: %v", err)
	}

	if len(store.persistCalls) != 1 {
		t.Fatalf("expected 1 persisted block, got %d", len(store.persistCalls))
	}
	got := store.persistCalls[0]
	if got.block.BlockNumber != 200 {
		t.Errorf("persisted block %d, want 200 (the checkpoint block)", got.block.BlockNumber)
	}
	if got.nextCheckpoint != 201 {
		t.Errorf("stored checkpoint %d, want 201 (next block to scan)", got.nextCheckpoint)
	}
}

// Reorg detection: the parent of the block about to be indexed must be
// compared against the last indexed block (checkpoint-1). On mismatch the
// stale block is orphaned and the checkpoint rewinds to re-index it.
func TestProcessTip_ReorgRewindsCheckpoint(t *testing.T) {
	chain := newFakeChain(190, 205)
	idx, store := newTestIndexer(t, 100, 200, chain)
	staleHash := "0xabababababababababababababababababababababababababababababababab"
	store.blocks[199] = &postgres.BlockRow{BlockNumber: 199, BlockHash: staleHash}

	if err := idx.processTip(context.Background(), 200, chain.headers[205]); err != nil {
		t.Fatalf("processTip: %v", err)
	}

	if len(store.deletedHashes) != 1 || store.deletedHashes[0] != staleHash {
		t.Fatalf("expected stale block %s orphaned, got deletions %v", staleHash, store.deletedHashes)
	}
	if len(store.setCheckpoints) != 1 || store.setCheckpoints[0] != 199 {
		t.Fatalf("expected checkpoint rewind to 199, got %v", store.setCheckpoints)
	}
	if len(store.persistCalls) != 0 {
		t.Fatalf("no block should be persisted during a reorg rewind, got %d", len(store.persistCalls))
	}
}
