package utils

import (
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
)

// FindEventNameByTopic0 parses the ABI JSON and returns the event name
// whose keccak256 signature matches the given topic0 hex string.
// Returns "" if no match found.
func FindEventNameByTopic0(abiJSON string, topic0 string) string {
	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return ""
	}

	// Normalize topic0 to lowercase with 0x prefix
	topic0 = strings.ToLower(topic0)
	if !strings.HasPrefix(topic0, "0x") {
		topic0 = "0x" + topic0
	}

	for _, ev := range parsed.Events {
		sig := ev.Sig
		hash := crypto.Keccak256Hash([]byte(sig))
		if strings.ToLower(hash.Hex()) == topic0 {
			return ev.Name
		}
	}

	return ""
}
