package solana

import (
	"testing"
)

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
	if events[0].LogIndex != 1 {
		t.Errorf("expected logIndex 1, got %d", events[0].LogIndex)
	}
	if events[0].DataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].DataBase64)
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
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].DataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].DataBase64)
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
		t.Fatalf("expected 1 event, got %d: %+v", len(events), events)
	}
	if events[0].DataBase64 != "AQIDBA==" {
		t.Errorf("unexpected data: %s", events[0].DataBase64)
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
	if events[0].DataBase64 != "AAAA" || events[1].DataBase64 != "BBBB" {
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

func TestExtractBlockLogs_FiltersPrograms(t *testing.T) {
	// base64 of 8+ bytes so the discriminator check passes
	// 0102030405060708 = "AQIDBAUGBwg="
	validData := "AQIDBAUGBwg="

	block := &Block{
		Slot:      500,
		Blockhash: "blockhash1",
		BlockTime: 1700000000,
		Transactions: []Transaction{
			{
				Signature:   "sig1",
				AccountKeys: []string{"TokenkegQ", "user1"},
				LogMessages: []string{
					"Program TokenkegQ invoke [1]",
					"Program data: " + validData,
					"Program TokenkegQ success",
				},
			},
			{
				Signature:   "sig2",
				AccountKeys: []string{"OtherProg", "user2"},
				LogMessages: []string{
					"Program OtherProg invoke [1]",
					"Program data: " + validData,
					"Program OtherProg success",
				},
			},
		},
	}

	programs := map[string]*ProgramInfo{
		"TokenkegQ": {
			ContractID:           1,
			DiscriminatorToEvent: map[string]string{},
		},
	}

	logs := ExtractBlockLogs(block, programs)
	if len(logs) != 1 {
		t.Fatalf("expected 1 log (only TokenkegQ), got %d", len(logs))
	}
	if logs[0].ProgramAddress != "TokenkegQ" {
		t.Errorf("expected TokenkegQ, got %s", logs[0].ProgramAddress)
	}
	if logs[0].TxHash != "sig1" {
		t.Errorf("expected sig1, got %s", logs[0].TxHash)
	}
}
