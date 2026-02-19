package utils

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// DecodeAddressFromTopic decodes an address from a 32-byte topic (indexed param).
// Topic is left-padded: 0x0000...0000 + 20-byte address.
func DecodeAddressFromTopic(topicHex string) (string, error) {
	if !strings.HasPrefix(topicHex, "0x") {
		topicHex = "0x" + topicHex
	}
	b := common.FromHex(topicHex)
	if len(b) < 20 {
		return "", fmt.Errorf("topic too short for address")
	}
	addr := common.BytesToAddress(b[len(b)-20:])
	return addr.Hex(), nil
}

// DecodeMemberAddedData decodes role, votingPower, timestamp from MemberAdded event data.
// Data layout: uint8 role (32 bytes) + uint256 votingPower (32 bytes) + uint256 timestamp (32 bytes).
func DecodeMemberAddedData(dataHex string) (role uint8, votingPower uint64, timestamp uint64, err error) {
	if !strings.HasPrefix(dataHex, "0x") {
		dataHex = "0x" + dataHex
	}
	data := common.FromHex(dataHex)
	if len(data) < 96 {
		return 0, 0, 0, fmt.Errorf("data too short: got %d bytes", len(data))
	}

	// ABI encoding: each value is 32 bytes, right-aligned
	role = data[31]
	votingPower = new(big.Int).SetBytes(data[32:64]).Uint64()
	timestamp = new(big.Int).SetBytes(data[64:96]).Uint64()
	return role, votingPower, timestamp, nil
}
