package solana

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// ProgramEvent is a raw event extracted from a transaction's logMessages.
type ProgramEvent struct {
	LogIndex   int
	DataBase64 string
}

// extractProgramEvents scans logMessages for "Program data:" lines emitted
// within the invocation scope of targetProgram at depth 1 only.
func extractProgramEvents(targetProgram string, logMessages []string) []ProgramEvent {
	var events []ProgramEvent
	targetDepth := 0
	currentDepth := 0

	for i, line := range logMessages {
		if strings.Contains(line, " invoke [") {
			depthStr := line[strings.LastIndex(line, "[")+1:]
			depthStr = strings.TrimSuffix(depthStr, "]")
			if d, err := strconv.Atoi(depthStr); err == nil {
				currentDepth = d
				if strings.HasPrefix(line, "Program "+targetProgram+" invoke [") {
					targetDepth = d
				}
			}
			continue
		}
		if strings.HasSuffix(line, " success") || strings.HasSuffix(line, " failed") {
			if strings.HasPrefix(line, "Program "+targetProgram+" ") {
				targetDepth = 0
			}
			if currentDepth > 0 {
				currentDepth--
			}
			continue
		}
		if targetDepth == 1 && currentDepth == 1 && strings.HasPrefix(line, "Program data: ") {
			data := strings.TrimPrefix(line, "Program data: ")
			events = append(events, ProgramEvent{LogIndex: i, DataBase64: data})
		}
	}

	return events
}

// ProcessedSolLog is a fully extracted Solana program event ready for persistence.
type ProcessedSolLog struct {
	ContractID     int64
	ProgramAddress string
	Slot           uint64
	BlockHash      string
	BlockTimestamp int64
	TxHash         string
	LogIndex       int
	Discriminator  string
	EventKey       string
	EventName      string
	RawData        string // base64
	DecodedParams  map[string]any
	AccountKeys    []string
}

// ProgramInfo holds the DB ID, IDL and event configs for a watched program.
type ProgramInfo struct {
	ContractID           int64
	IDL                  string
	DiscriminatorToEvent map[string]string // discriminator hex -> event name
}

// ExtractBlockLogs extracts all matching program events from a Block.
func ExtractBlockLogs(block *Block, programs map[string]*ProgramInfo) []ProcessedSolLog {
	if block == nil {
		return nil
	}

	var out []ProcessedSolLog

	for _, tx := range block.Transactions {
		accountSet := make(map[string]struct{}, len(tx.AccountKeys))
		for _, k := range tx.AccountKeys {
			accountSet[k] = struct{}{}
		}
		accountKeysJSON, _ := json.Marshal(tx.AccountKeys)
		_ = accountKeysJSON

		for programAddr, pInfo := range programs {
			if _, ok := accountSet[programAddr]; !ok {
				continue
			}

			events := extractProgramEvents(programAddr, tx.LogMessages)
			for _, pe := range events {
				rawBytes, err := base64.StdEncoding.DecodeString(pe.DataBase64)
				if err != nil {
					continue
				}
				if len(rawBytes) < 8 {
					continue
				}

				discriminatorHex := hex.EncodeToString(rawBytes[:8])
				eventKey := programAddr + ":" + discriminatorHex
				eventName := pInfo.DiscriminatorToEvent[discriminatorHex]

				out = append(out, ProcessedSolLog{
					ContractID:     pInfo.ContractID,
					ProgramAddress: programAddr,
					Slot:           block.Slot,
					BlockHash:      block.Blockhash,
					BlockTimestamp: block.BlockTime,
					TxHash:         tx.Signature,
					LogIndex:       pe.LogIndex,
					Discriminator:  discriminatorHex,
					EventKey:       eventKey,
					EventName:      eventName,
					RawData:        pe.DataBase64,
					AccountKeys:    tx.AccountKeys,
				})
			}
		}
	}

	return out
}
