package solana

import (
	"context"
	"testing"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// mockRPC simulates getSlot and getBlocksWithLimit for subscriber tests.
type mockRPC struct {
	slots    []uint64 // sequence of values returned by GetSlot calls
	slotIdx  int
	// blocks maps startSlot → confirmed slot list
	blocks map[uint64][]uint64
}

func (m *mockRPC) GetSlot(ctx context.Context) (uint64, error) {
	if m.slotIdx >= len(m.slots) {
		return m.slots[len(m.slots)-1], nil
	}
	s := m.slots[m.slotIdx]
	m.slotIdx++
	return s, nil
}

func (m *mockRPC) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	if slots, ok := m.blocks[startSlot]; ok {
		return slots, nil
	}
	return nil, nil
}

func (m *mockRPC) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	return &Block{Slot: slot, Blockhash: "hash", BlockTime: 1000}, nil
}

func collectEvents(out <-chan core.BlockEvent, n int, timeout time.Duration) []core.BlockEvent {
	var events []core.BlockEvent
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(events) < n {
		select {
		case ev, ok := <-out:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-timer.C:
			return events
		}
	}
	return events
}

func TestSubscriber_BackfillsFromSlot(t *testing.T) {
	rpc := &mockRPC{
		slots: []uint64{105, 105},
		blocks: map[uint64][]uint64{
			100: {100, 101, 103, 104, 105}, // slot 102 has no block
		},
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 100, out) //nolint:errcheck

	events := collectEvents(out, 5, 500*time.Millisecond)
	cancel()

	if len(events) < 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	slots := make([]uint64, len(events))
	for i, ev := range events {
		slots[i] = ev.Number
	}
	expected := []uint64{100, 101, 103, 104, 105}
	for i, want := range expected {
		if slots[i] != want {
			t.Errorf("event[%d]: want slot %d, got %d", i, want, slots[i])
		}
	}
}

func TestSubscriber_SkipsEmptySlots(t *testing.T) {
	rpc := &mockRPC{
		slots:  []uint64{200},
		blocks: map[uint64][]uint64{150: {150, 152, 155}},
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 150, out) //nolint:errcheck

	events := collectEvents(out, 3, 500*time.Millisecond)
	cancel()

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	for i, wantSlot := range []uint64{150, 152, 155} {
		if events[i].Number != wantSlot {
			t.Errorf("event[%d]: want %d, got %d", i, wantSlot, events[i].Number)
		}
	}
}

func TestSubscriber_SetsBlockHash(t *testing.T) {
	rpc := &mockRPC{
		slots:  []uint64{10},
		blocks: map[uint64][]uint64{10: {10}},
	}
	sub := NewSubscriber(rpc, 10*time.Millisecond)
	out := make(chan core.BlockEvent, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sub.SubscribeBlocks(ctx, 10, out) //nolint:errcheck

	events := collectEvents(out, 1, 500*time.Millisecond)
	cancel()

	if len(events) == 0 {
		t.Fatal("no events received")
	}
	if events[0].Hash == "" {
		t.Error("expected non-empty block hash")
	}
}
