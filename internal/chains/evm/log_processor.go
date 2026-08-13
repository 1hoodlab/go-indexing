package evm

import (
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/pkg/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type ProcessedLog struct {
	ContractID       int64
	ContractAddress  string
	BlockNumber      uint64
	BlockHash        string
	ParentHash       string
	BlockTimestamp   time.Time
	TransactionHash  string
	TransactionIndex int
	LogIndex         int
	EventName        string
	Topics           []string
	Data             string
	DecodedParams    map[string]any
}

type LogProcessor struct {
	allowedTopic0ByAddr   map[string]map[string]struct{}
	abiByAddr             map[string]string
	eventNameByAddrTopic0 map[string]map[string]string
	contractIDByAddr      map[string]int64
}

func NewLogProcessor(
	allowed map[string]map[string]struct{},
	abiByAddr map[string][]byte,
	eventNameByAddrTopic0 map[string]map[string]string,
	contractIDByAddr map[string]int64,
) *LogProcessor {
	abiStr := make(map[string]string, len(abiByAddr))
	for k, v := range abiByAddr {
		abiStr[k] = string(v)
	}
	return &LogProcessor{
		allowedTopic0ByAddr:   allowed,
		abiByAddr:             abiStr,
		eventNameByAddrTopic0: eventNameByAddrTopic0,
		contractIDByAddr:      contractIDByAddr,
	}
}

func (p *LogProcessor) ProcessLogs(logs []types.Log, blockTimestamp time.Time) []ProcessedLog {
	var out []ProcessedLog
	for _, vLog := range logs {
		addr := strings.ToLower(vLog.Address.Hex())
		topic0 := ""
		if len(vLog.Topics) > 0 {
			topic0 = strings.ToLower(vLog.Topics[0].Hex())
		}
		allowed, ok := p.allowedTopic0ByAddr[addr]
		if !ok {
			continue
		}
		if _, ok := allowed[topic0]; !ok {
			continue
		}

		eventName := ""
		if m, ok := p.eventNameByAddrTopic0[addr]; ok {
			eventName = m[topic0]
		}

		topics := make([]string, len(vLog.Topics))
		for i, t := range vLog.Topics {
			topics[i] = strings.ToLower(t.Hex())
		}

		dataHex := "0x" + common.Bytes2Hex(vLog.Data)
		var decoded map[string]any
		if abiJSON := p.abiByAddr[addr]; abiJSON != "" && eventName != "" {
			if params, err := utils.DecodeEventLog(abiJSON, eventName, topics, dataHex); err == nil {
				decoded = stringifyBigInts(params)
			}
		}

		out = append(out, ProcessedLog{
			ContractID:       p.contractIDByAddr[addr],
			ContractAddress:  addr,
			BlockNumber:      vLog.BlockNumber,
			BlockHash:        strings.ToLower(vLog.BlockHash.Hex()),
			BlockTimestamp:   blockTimestamp,
			TransactionHash:  strings.ToLower(vLog.TxHash.Hex()),
			TransactionIndex: int(vLog.TxIndex),
			LogIndex:         int(vLog.Index),
			EventName:        eventName,
			Topics:           topics,
			Data:             dataHex,
			DecodedParams:    decoded,
		})
	}
	return out
}

func stringifyBigInts(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		switch val := v.(type) {
		case *big.Int:
			if val != nil {
				out[k] = val.String()
			}
		default:
			out[k] = v
		}
	}
	return out
}

// MarshalTopicsJSON for postgres insert.
func MarshalTopicsJSON(topics []string) ([]byte, error) {
	return json.Marshal(topics)
}
