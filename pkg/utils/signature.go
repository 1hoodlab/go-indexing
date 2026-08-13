package utils

import (
	"encoding/hex"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// EventNameFromSignature returns the name before '('.
func EventNameFromSignature(canonical string) string {
	if i := strings.Index(canonical, "("); i > 0 {
		return canonical[:i]
	}
	return canonical
}

// EventTopic0 returns the topic0 (event signature hash) for a canonical event string.
// Canonical format: EventName(type1,type2,...) with no spaces.
// Example: EventTopic0("Transfer(address,address,uint256)") -> "0xddf252ad..."
func EventTopic0(canonical string) string {
	hash := crypto.Keccak256Hash([]byte(canonical))
	return hash.Hex()
}

// EventTopic0Bytes returns topic0 as 32-byte slice for FilterQuery.
func EventTopic0Bytes(canonical string) []byte {
	hash := crypto.Keccak256Hash([]byte(canonical))
	return hash.Bytes()
}

// EventTopic0FromBytes decodes hex bytes to topic0 string.
func EventTopic0FromBytes(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}
