package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

type mockSubscriber struct {
	blocks []core.BlockEvent
}

func (m *mockSubscriber) SubscribeBlocks(ctx context.Context, from uint64, out chan<- core.BlockEvent) error {
	for _, b := range m.blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- b:
		}
	}
	return nil
}

type mockFetcher struct {
	events map[uint64][]core.RawEvent
}

func (m *mockFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	return m.events[block.Number], nil
}

type mockStorage struct {
	savedEvents  []core.RawEvent
	scanProgress map[string]uint64
}

func (m *mockStorage) SaveEvents(ctx context.Context, events []core.RawEvent) error {
	m.savedEvents = append(m.savedEvents, events...)
	return nil
}
func (m *mockStorage) UpdateScanProgress(ctx context.Context, address string, block uint64) error {
	if m.scanProgress == nil {
		m.scanProgress = make(map[string]uint64)
	}
	m.scanProgress[address] = block
	return nil
}
func (m *mockStorage) LoadContracts(ctx context.Context) ([]core.ContractConfig, error) {
	return nil, nil
}
func (m *mockStorage) EnsureIndexes(ctx context.Context) error { return nil }
func (m *mockStorage) Close(ctx context.Context) error         { return nil }

func TestPipeline_ProcessesBlocksAndDispatches(t *testing.T) {
	sub := &mockSubscriber{blocks: []core.BlockEvent{{Number: 100}, {Number: 101}}}

	fetcher := &mockFetcher{events: map[uint64][]core.RawEvent{
		100: {
			{BlockNumber: 100, EventKey: "0xabc", Address: "0x1", Metadata: map[string]string{}},
			{BlockNumber: 100, EventKey: "0xabc", Address: "0x1", Metadata: map[string]string{}},
			{BlockNumber: 100, EventKey: "0xdef", Address: "0x1", Metadata: map[string]string{}},
		},
		101: {},
	}}

	store := &mockStorage{}

	var abcCount, defCount int
	registry := core.NewRegistry(nil)
	registry.Register("0xabc", func(ctx context.Context, batch []core.RawEvent) error {
		abcCount += len(batch)
		return nil
	})
	registry.Register("0xdef", func(ctx context.Context, batch []core.RawEvent) error {
		defCount += len(batch)
		return nil
	})

	contracts := []core.ContractConfig{{Address: "0x1", StartBlock: 100}}

	p := New("test", sub, fetcher, registry, store, contracts)
	if err := p.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	p.Stop()

	if abcCount != 2 {
		t.Errorf("expected 2 events for 0xabc, got %d", abcCount)
	}
	if defCount != 1 {
		t.Errorf("expected 1 event for 0xdef, got %d", defCount)
	}
	if store.scanProgress["0x1"] != 101 {
		t.Errorf("expected scan progress 101 for 0x1, got %d", store.scanProgress["0x1"])
	}
}
