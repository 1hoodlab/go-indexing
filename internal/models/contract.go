package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Contract stores contract config for indexing. Each contract belongs to one network.
type Contract struct {
	ID                primitive.ObjectID `bson:"_id,omitempty"`
	NetworkID         primitive.ObjectID `bson:"network_id"`
	Address           string             `bson:"address"` // lowercase, 0x-prefix
	ABI               string             `bson:"abi"`    // minified JSON
	Events            []string           `bson:"events"` // e.g. Transfer(address,address,uint256)
	StartBlock        uint64             `bson:"start_block"`
	LastScannedBlock  uint64             `bson:"last_scanned_block"` // if 0, start from start_block
	Status            NetworkStatus      `bson:"status"`
	CreatedAt         time.Time          `bson:"created_at"`
	UpdatedAt         time.Time          `bson:"updated_at"`
}

// NextBlock returns the block number to scan next.
// If last_scanned_block == 0, returns start_block; else returns last_scanned_block + 1.
func (c *Contract) NextBlock() uint64 {
	if c.LastScannedBlock == 0 {
		return c.StartBlock
	}
	return c.LastScannedBlock + 1
}

// CollectionName returns the MongoDB collection name.
func (Contract) CollectionName() string {
	return "contracts"
}
