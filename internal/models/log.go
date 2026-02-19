package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Log stores an indexed event log. Traceable to network and contract.
type Log struct {
	ID              primitive.ObjectID `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID `bson:"network_id"`
	ContractID      primitive.ObjectID `bson:"contract_id"`
	BlockNumber     uint64             `bson:"block_number"`
	BlockHash       string             `bson:"block_hash"`
	TransactionHash string             `bson:"transaction_hash"`
	LogIndex        uint32             `bson:"log_index"`
	Address         string             `bson:"address"` // contract address (redundant for queries)
	EventSignature  string                 `bson:"event_signature"` // topic0
	Topics          []string               `bson:"topics"`         // topic1, topic2, topic3
	Data            string                 `bson:"data"`           // hex
	DataDecoded     map[string]interface{} `bson:"data_decoded,omitempty"` // decoded params (when ABI decode succeeds)
	CreatedAt       time.Time              `bson:"created_at"`
}

// CollectionName returns the MongoDB collection name.
func (Log) CollectionName() string {
	return "logs"
}
