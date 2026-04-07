package solana

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// programEvent is a raw event extracted from a transaction's logMessages.
type programEvent struct {
	logIndex   int
	dataBase64 string
}

// extractProgramEvents scans logMessages for "Program data:" lines emitted
// within the invocation scope of targetProgram at depth 1 only (not CPIs).
func extractProgramEvents(targetProgram string, logMessages []string) []programEvent {
	var events []programEvent
	inScope := false
	// nestedDepth tracks how many inner program invocations we are inside
	// while inScope is true. When nestedDepth > 0 we suppress "Program data:" lines.
	nestedDepth := 0

	for i, line := range logMessages {
		switch {
		case strings.HasPrefix(line, "Program "+targetProgram+" invoke ["):
			depthStr := strings.TrimPrefix(line, "Program "+targetProgram+" invoke [")
			depthStr = strings.TrimSuffix(depthStr, "]")
			if depth, err := strconv.Atoi(depthStr); err == nil && depth == 1 {
				inScope = true
				nestedDepth = 0
			}
		case line == "Program "+targetProgram+" success" ||
			line == "Program "+targetProgram+" failed":
			inScope = false
			nestedDepth = 0
		case inScope && strings.Contains(line, " invoke ["):
			// A nested program invocation — track depth to suppress its data lines.
			nestedDepth++
		case inScope && nestedDepth > 0 && (strings.HasSuffix(line, " success") || strings.HasSuffix(line, " failed")):
			nestedDepth--
		case inScope && nestedDepth == 0 && strings.HasPrefix(line, "Program data: "):
			data := strings.TrimPrefix(line, "Program data: ")
			events = append(events, programEvent{logIndex: i, dataBase64: data})
		}
	}

	return events
}

// programInfo holds the MongoDB ID and IDL for a known program.
type programInfo struct {
	id  string
	idl string
}

// ProgramInfoExport is the exported version of programInfo for use in main.go.
type ProgramInfoExport struct {
	ID  string
	IDL string
}

// LogFetcher implements core.LogFetcher for Solana.
type LogFetcher struct {
	client     SolanaRPCClient
	networkID  string
	programMap map[string]programInfo // key: base58 program address
}

// NewLogFetcher creates a LogFetcher.
func NewLogFetcher(client SolanaRPCClient, networkID string, programMap map[string]programInfo) *LogFetcher {
	return &LogFetcher{client: client, networkID: networkID, programMap: programMap}
}

// NewLogFetcherFromExport creates a LogFetcher from the exported ProgramInfoExport map.
func NewLogFetcherFromExport(client SolanaRPCClient, networkID string, programs map[string]ProgramInfoExport) *LogFetcher {
	pm := make(map[string]programInfo, len(programs))
	for addr, p := range programs {
		pm[addr] = programInfo{id: p.ID, idl: p.IDL}
	}
	return NewLogFetcher(client, networkID, pm)
}

// FetchLogs implements core.LogFetcher.
func (f *LogFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	b, err := f.client.GetBlock(ctx, block.Number)
	if err != nil {
		return nil, fmt.Errorf("solana fetcher: getBlock(%d): %w", block.Number, err)
	}
	if b == nil {
		return nil, nil
	}
	return f.fetchLogsFromBlock(ctx, *b, contracts)
}

// fetchLogsFromBlock is separated for unit testing without a real RPC client.
// ctx is reserved for future cancellation support.
func (f *LogFetcher) fetchLogsFromBlock(_ context.Context, b Block, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	// Build active programs for this slot
	activePrograms := make(map[string]core.ContractConfig)
	for _, c := range contracts {
		nextSlot := c.StartBlock
		if c.LastScanned > 0 {
			nextSlot = c.LastScanned + 1
		}
		if b.Slot >= nextSlot {
			activePrograms[c.Address] = c
		}
	}
	if len(activePrograms) == 0 {
		return nil, nil
	}

	var events []core.RawEvent

	for _, tx := range b.Transactions {
		accountSet := make(map[string]struct{}, len(tx.AccountKeys))
		for _, k := range tx.AccountKeys {
			accountSet[k] = struct{}{}
		}
		accountKeysJSON, jsonErr := json.Marshal(tx.AccountKeys)
		if jsonErr != nil {
			accountKeysJSON = []byte("[]")
		}

		for programAddr := range activePrograms {
			if _, ok := accountSet[programAddr]; !ok {
				continue
			}
			pi, known := f.programMap[programAddr]
			if !known {
				continue
			}

			programEvents := extractProgramEvents(programAddr, tx.LogMessages)
			for _, pe := range programEvents {
				// Solana RPC uses standard base64 with padding for Program data log lines.
				rawBytes, err := base64.StdEncoding.DecodeString(pe.dataBase64)
				if err != nil {
					log.Printf("solana fetcher: slot %d tx %s: invalid base64 in program data: %v", b.Slot, tx.Signature, err)
					continue
				}
				if len(rawBytes) < 8 {
					continue
				}

				discriminatorHex := hex.EncodeToString(rawBytes[:8])
				eventKey := programAddr + ":" + discriminatorHex

				events = append(events, core.RawEvent{
					BlockNumber:     b.Slot,
					TransactionHash: tx.Signature,
					Address:         programAddr,
					EventKey:        eventKey,
					RawData:         rawBytes,
					Metadata: map[string]string{
						"network_id":   f.networkID,
						"program_id":   pi.id,
						"block_hash":   b.Blockhash,
						"slot":         strconv.FormatUint(b.Slot, 10),
						"log_index":    strconv.Itoa(pe.logIndex),
						"account_keys": string(accountKeysJSON),
						"idl":          pi.idl,
					},
				})
			}
		}
	}

	return events, nil
}
