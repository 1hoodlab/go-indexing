package evm

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type stubRPC struct {
	receiptCalls int
	receipts     []RpcReceipt
}

func (s *stubRPC) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return &types.Header{}, nil
}
func (s *stubRPC) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return types.NewBlock(&types.Header{}, nil, nil, nil), nil
}
func (s *stubRPC) FilterLogs(ctx context.Context, from, to *big.Int, addresses []common.Address) ([]types.Log, error) {
	return nil, nil
}
func (s *stubRPC) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error) {
	s.receiptCalls++
	if s.receiptCalls < 3 {
		return nil, nil
	}
	return s.receipts, nil
}

func TestGetBlockReceiptsWithRetry_RetriesUntilReady(t *testing.T) {
	stub := &stubRPC{receipts: []RpcReceipt{{Logs: []types.Log{{}}}}}
	receipts, err := GetBlockReceiptsWithRetry(context.Background(), stub, common.Hash{}, 5, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(receipts) != 1 || stub.receiptCalls != 3 {
		t.Fatalf("want 3 calls and 1 receipt, got calls=%d receipts=%d", stub.receiptCalls, len(receipts))
	}
}

func TestFlattenReceiptLogs(t *testing.T) {
	receipts := []RpcReceipt{
		{Logs: []types.Log{{Index: 1}}},
		{Logs: []types.Log{{Index: 2}, {Index: 3}}},
	}
	logs := FlattenReceiptLogs(receipts)
	if len(logs) != 3 {
		t.Fatalf("want 3 logs, got %d", len(logs))
	}
}
