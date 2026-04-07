package solana

import (
	"context"
	"strings"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// --- extractProgramEvents tests ---

func TestExtractProgramEvents_BasicScope(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].logIndex != 1 {
		t.Errorf("expected logIndex 1, got %d", events[0].logIndex)
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_IgnoresOtherPrograms(t *testing.T) {
	logs := []string{
		"Program OtherProg invoke [1]",
		"Program data: AAAA",
		"Program OtherProg success",
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event (only from TokenkegQ), got %d", len(events))
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_NestedCPI(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AQIDBA==",
		"Program InnerProg invoke [2]",
		"Program data: ZZZZ",
		"Program InnerProg success",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 1 {
		t.Fatalf("expected 1 event (outer program only), got %d: %+v", len(events), events)
	}
	if events[0].dataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].dataBase64)
	}
}

func TestExtractProgramEvents_MultipleEventsInOneTx(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program data: AAAA",
		"Program data: BBBB",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].dataBase64 != "AAAA" || events[1].dataBase64 != "BBBB" {
		t.Errorf("unexpected data: %+v", events)
	}
}

func TestExtractProgramEvents_NoEvents(t *testing.T) {
	logs := []string{
		"Program TokenkegQ invoke [1]",
		"Program log: some message",
		"Program TokenkegQ success",
	}
	events := extractProgramEvents("TokenkegQ", logs)
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

// --- LogFetcher tests ---

func makeMockBlock(programID string, logs []string) Block {
	return Block{
		Slot:      500,
		Blockhash: "blockhash1",
		BlockTime: 1700000000,
		Transactions: []Transaction{
			{
				Signature:   "sig1",
				AccountKeys: []string{programID, "user1"},
				LogMessages: logs,
			},
		},
	}
}

func TestLogFetcher_ExtractsRawEvents(t *testing.T) {
	programID := "TokenkegQ"
	block := makeMockBlock(programID, []string{
		"Program " + programID + " invoke [1]",
		"Program data: AAECAwQFBgc=",
		"Program " + programID + " success",
	})

	fetcher := NewLogFetcher(nil, "netid123", map[string]programInfo{
		programID: {id: "progid1", idl: ""},
	})

	contracts := []core.ContractConfig{
		{Address: programID, StartBlock: 100, LastScanned: 400},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.Address != programID {
		t.Errorf("expected address %s, got %s", programID, ev.Address)
	}
	if ev.BlockNumber != 500 {
		t.Errorf("expected block 500, got %d", ev.BlockNumber)
	}
	if ev.TransactionHash != "sig1" {
		t.Errorf("expected sig1, got %s", ev.TransactionHash)
	}
	if !strings.HasPrefix(ev.EventKey, programID+":") {
		t.Errorf("unexpected EventKey format: %s", ev.EventKey)
	}
	if len(ev.RawData) == 0 {
		t.Error("expected non-empty RawData")
	}
	if ev.Metadata["network_id"] != "netid123" {
		t.Errorf("expected network_id netid123, got %s", ev.Metadata["network_id"])
	}
}

func TestLogFetcher_SkipsProgramNotInContracts(t *testing.T) {
	block := makeMockBlock("OtherProg", []string{
		"Program OtherProg invoke [1]",
		"Program data: AQIDBA==",
		"Program OtherProg success",
	})

	fetcher := NewLogFetcher(nil, "netid", map[string]programInfo{
		"TokenkegQ": {id: "progid1"},
	})
	contracts := []core.ContractConfig{
		{Address: "TokenkegQ", StartBlock: 100},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events (program not in contracts), got %d", len(events))
	}
}

func TestLogFetcher_SkipsSlotBeforeStartBlock(t *testing.T) {
	programID := "TokenkegQ"
	block := makeMockBlock(programID, []string{
		"Program " + programID + " invoke [1]",
		"Program data: AQIDBA==",
		"Program " + programID + " success",
	})
	block.Slot = 50 // before StartBlock

	fetcher := NewLogFetcher(nil, "netid", map[string]programInfo{
		programID: {id: "progid1"},
	})
	contracts := []core.ContractConfig{
		{Address: programID, StartBlock: 100},
	}

	events, err := fetcher.fetchLogsFromBlock(context.Background(), block, contracts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events (slot before start), got %d", len(events))
	}
}
