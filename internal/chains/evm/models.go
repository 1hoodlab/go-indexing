package evm

import (
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Contract struct {
	ID               primitive.ObjectID   `bson:"_id,omitempty"`
	NetworkID        primitive.ObjectID   `bson:"network_id"`
	Address          string               `bson:"address"`
	ABI              string               `bson:"abi"`
	Events           []string             `bson:"events"`
	StartBlock       uint64               `bson:"start_block"`
	LastScannedBlock uint64               `bson:"last_scanned_block"`
	Status           models.NetworkStatus `bson:"status"`
	CreatedAt        time.Time            `bson:"created_at"`
	UpdatedAt        time.Time            `bson:"updated_at"`
}

func (c *Contract) NextBlock() uint64 {
	if c.LastScannedBlock == 0 {
		return c.StartBlock
	}
	return c.LastScannedBlock + 1
}

type Log struct {
	ID              primitive.ObjectID     `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ContractID      primitive.ObjectID     `bson:"contract_id"`
	BlockNumber     uint64                 `bson:"block_number"`
	BlockHash       string                 `bson:"block_hash"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        uint32                 `bson:"log_index"`
	Address         string                 `bson:"address"`
	EventSignature  string                 `bson:"event_signature"`
	Topics          []string               `bson:"topics"`
	Data            string                 `bson:"data"`
	DataDecoded     map[string]interface{} `bson:"data_decoded,omitempty"`
	CreatedAt       time.Time              `bson:"created_at"`
}
