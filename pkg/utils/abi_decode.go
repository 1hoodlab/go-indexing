package utils

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// DecodeEventLog decodes an event log using the contract ABI from schema.
// fullTopics: [topic0, topic1, topic2, topic3] (topic0 = event signature)
// data: hex string of non-indexed params
// Returns map of param name -> decoded value.
func DecodeEventLog(abiJSON, eventName string, fullTopics []string, data string) (map[string]interface{}, error) {
	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return nil, fmt.Errorf("parse ABI: %w", err)
	}

	// Find event by name (ABI may use full sig as key)
	ev, ok := parsed.Events[eventName]
	if !ok {
		for sig, e := range parsed.Events {
			if strings.HasPrefix(sig, eventName+"(") {
				ev = e
				ok = true
				break
			}
		}
	}
	if !ok {
		return nil, fmt.Errorf("event %s not found in ABI", eventName)
	}

	result := make(map[string]interface{})
	topics := fullTopics
	if len(topics) > 0 && !strings.HasPrefix(topics[0], "0x") {
		topics[0] = "0x" + topics[0]
	}

	// Decode indexed params from topics (topic0 is event sig, topic1+ are indexed)
	idxTopic := 1
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			continue
		}
		if idxTopic >= len(topics) {
			break
		}
		topicHex := topics[idxTopic]
		if !strings.HasPrefix(topicHex, "0x") {
			topicHex = "0x" + topicHex
		}
		b := common.FromHex(topicHex)
		val, err := decodeTopicByType(&arg.Type, b)
		if err != nil {
			return nil, fmt.Errorf("decode indexed %s: %w", arg.Name, err)
		}
		result[arg.Name] = val
		idxTopic++
	}

	// Decode non-indexed params from data
	dataBytes := common.FromHex(data)
	if len(dataBytes) > 0 {
		nonIndexed := ev.Inputs.NonIndexed()
		if len(nonIndexed) > 0 {
			vals, err := nonIndexed.Unpack(dataBytes)
			if err != nil {
				return nil, fmt.Errorf("unpack data: %w", err)
			}
			for i, arg := range nonIndexed {
				if i < len(vals) {
					result[arg.Name] = vals[i]
				}
			}
		}
	}

	return result, nil
}

// decodeTopicByType decodes a 32-byte topic into the appropriate type.
func decodeTopicByType(t *abi.Type, b []byte) (interface{}, error) {
	if len(b) < 32 {
		return nil, fmt.Errorf("topic too short")
	}
	if len(b) > 32 {
		b = b[len(b)-32:]
	}

	switch t.T {
	case abi.AddressTy:
		addr := common.BytesToAddress(b[12:])
		return addr.Hex(), nil
	case abi.IntTy, abi.UintTy:
		n := new(big.Int).SetBytes(b)
		if t.Size <= 64 {
			return n.Uint64(), nil
		}
		return n.String(), nil
	case abi.BoolTy:
		return b[31] != 0, nil
	case abi.BytesTy, abi.FixedBytesTy:
		return common.BytesToHash(b).Hex(), nil
	default:
		return common.BytesToHash(b).Hex(), nil
	}
}
