package solana

import (
	"context"
	"errors"
	"testing"
)

var errFakeRPC = errors.New("fake rpc error")

type mockSolanaClient struct {
	slot uint64
	fail bool
}

func (m *mockSolanaClient) GetSlot(ctx context.Context) (uint64, error) {
	if m.fail {
		return 0, errFakeRPC
	}
	return m.slot, nil
}

func (m *mockSolanaClient) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	if m.fail {
		return nil, errFakeRPC
	}
	slots := make([]uint64, 0, limit)
	for i := uint64(0); i < limit; i++ {
		slots = append(slots, startSlot+i)
	}
	return slots, nil
}

func (m *mockSolanaClient) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	if m.fail {
		return nil, errFakeRPC
	}
	return &Block{Slot: slot, Blockhash: "testhash"}, nil
}

func TestRPCPool_UsesFirstURL(t *testing.T) {
	good := &mockSolanaClient{slot: 100}
	dialFn := func(url string) (SolanaRPCClient, error) {
		if url == "http://good" {
			return good, nil
		}
		return nil, errFakeRPC
	}

	pool, err := NewRPCPoolWithDialFn([]string{"http://good", "http://bad"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slot, err := pool.GetSlot(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot != 100 {
		t.Fatalf("expected 100, got %d", slot)
	}
}

func TestRPCPool_RotatesOnError(t *testing.T) {
	bad := &mockSolanaClient{fail: true}
	good := &mockSolanaClient{slot: 200}
	dialFn := func(url string) (SolanaRPCClient, error) {
		if url == "http://bad" {
			return bad, nil
		}
		return good, nil
	}

	pool, err := NewRPCPoolWithDialFn([]string{"http://bad", "http://good"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	slot, err := pool.GetSlot(context.Background())
	if err != nil {
		t.Fatalf("expected rotation to succeed: %v", err)
	}
	if slot != 200 {
		t.Fatalf("expected 200 after rotation, got %d", slot)
	}
}

func TestRPCPool_ErrorsWhenAllFail(t *testing.T) {
	dialFn := func(url string) (SolanaRPCClient, error) {
		return &mockSolanaClient{fail: true}, nil
	}
	pool, err := NewRPCPoolWithDialFn([]string{"http://a", "http://b"}, dialFn)
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	_, err = pool.GetSlot(context.Background())
	if !errors.Is(err, ErrAllRPCsFailed) {
		t.Fatalf("expected ErrAllRPCsFailed, got %v", err)
	}
}

func TestRPCPool_NoURLsErrors(t *testing.T) {
	_, err := NewRPCPoolWithDialFn([]string{}, func(url string) (SolanaRPCClient, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrNoRPCURLs) {
		t.Fatalf("expected ErrNoRPCURLs, got %v", err)
	}
}
