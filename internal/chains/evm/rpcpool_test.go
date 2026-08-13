package evm

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type mockRPCClient struct {
	url       string
	failCalls bool
}

func (m *mockRPCClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if m.failCalls {
		return nil, errors.New("rpc error")
	}
	return &types.Header{}, nil
}

func (m *mockRPCClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	if m.failCalls {
		return nil, errors.New("rpc error")
	}
	return types.NewBlock(&types.Header{}, nil, nil, nil), nil
}

func (m *mockRPCClient) FilterLogs(ctx context.Context, from, to *big.Int, addresses []common.Address) ([]types.Log, error) {
	if m.failCalls {
		return nil, errors.New("rpc error")
	}
	return []types.Log{}, nil
}

func (m *mockRPCClient) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error) {
	if m.failCalls {
		return nil, errors.New("rpc error")
	}
	return []RpcReceipt{}, nil
}

func TestRPCPool_UsesFirstURL(t *testing.T) {
	dialCalls := []string{}
	dialFn := func(url string) (RPCClient, error) {
		dialCalls = append(dialCalls, url)
		return &mockRPCClient{url: url}, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"url1", "url2", "url3"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool.ActiveURL() != "url1" {
		t.Fatalf("expected active url1, got %s", pool.ActiveURL())
	}
	if len(dialCalls) != 1 {
		t.Fatalf("expected 1 dial call, got %d", len(dialCalls))
	}
}

func TestRPCPool_RotatesOnFailure(t *testing.T) {
	callCount := 0
	dialFn := func(url string) (RPCClient, error) {
		callCount++
		if url == "url1" {
			return &mockRPCClient{url: url, failCalls: true}, nil
		}
		return &mockRPCClient{url: url}, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"url1", "url2", "url3"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Call should fail on url1, rotate to url2, succeed
	_, err = pool.HeaderByNumber(context.Background(), big.NewInt(1))
	if err != nil {
		t.Fatalf("expected success after fallback, got: %v", err)
	}
	if pool.ActiveURL() != "url2" {
		t.Fatalf("expected active url2, got %s", pool.ActiveURL())
	}
}

func TestRPCPool_ReturnsErrorWhenAllFail(t *testing.T) {
	dialFn := func(url string) (RPCClient, error) {
		return &mockRPCClient{url: url, failCalls: true}, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"url1", "url2"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = pool.HeaderByNumber(context.Background(), big.NewInt(1))
	if !errors.Is(err, ErrAllRPCsFailed) {
		t.Fatalf("expected ErrAllRPCsFailed, got: %v", err)
	}
}

func TestRPCPool_RetriesFallbacksOnEachCall(t *testing.T) {
	dialCalls := []string{}
	dialFn := func(url string) (RPCClient, error) {
		dialCalls = append(dialCalls, url)
		return &mockRPCClient{url: url, failCalls: true}, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"url1", "url2"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pool.HeaderByNumber(context.Background(), big.NewInt(1))

	dialCalls = []string{}
	_, err = pool.HeaderByNumber(context.Background(), big.NewInt(2))
	if !errors.Is(err, ErrAllRPCsFailed) {
		t.Fatalf("expected ErrAllRPCsFailed, got: %v", err)
	}
	if len(dialCalls) != 0 {
		t.Fatalf("expected no extra dial calls on retry, got %v", dialCalls)
	}
}
