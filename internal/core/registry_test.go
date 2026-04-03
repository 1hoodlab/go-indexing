package core

import (
	"context"
	"testing"
)

func TestRegistry_DispatchesToRegisteredHandler(t *testing.T) {
	var called bool
	var gotBatch []RawEvent

	reg := NewRegistry(nil)
	reg.Register("0xabc", func(ctx context.Context, batch []RawEvent) error {
		called = true
		gotBatch = batch
		return nil
	})

	batch := []RawEvent{{EventKey: "0xabc", Address: "0x1"}}
	if err := reg.Dispatch(context.Background(), "0xabc", batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called")
	}
	if len(gotBatch) != 1 || gotBatch[0].Address != "0x1" {
		t.Fatalf("unexpected batch: %+v", gotBatch)
	}
}

func TestRegistry_FallsBackToDefault(t *testing.T) {
	var defaultCalled bool

	reg := NewRegistry(func(ctx context.Context, batch []RawEvent) error {
		defaultCalled = true
		return nil
	})

	batch := []RawEvent{{EventKey: "unknown"}}
	if err := reg.Dispatch(context.Background(), "unknown_key", batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !defaultCalled {
		t.Fatal("expected default handler to be called")
	}
}

func TestRegistry_EmptyBatchIsNoop(t *testing.T) {
	var called bool

	reg := NewRegistry(func(ctx context.Context, batch []RawEvent) error {
		called = true
		return nil
	})
	reg.Register("0xabc", func(ctx context.Context, batch []RawEvent) error {
		called = true
		return nil
	})

	if err := reg.Dispatch(context.Background(), "0xabc", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("expected no handler to be called for empty batch")
	}
}
