package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// NetworkType represents mainnet or testnet.
type NetworkType string

const (
	NetworkMainnet NetworkType = "mainnet"
	NetworkTestnet NetworkType = "testnet"
)

// NetworkStatus represents active or deactive state.
type NetworkStatus string

const (
	StatusActive   NetworkStatus = "active"
	StatusDeactive NetworkStatus = "deactive"
)

// Network stores blockchain network config: RPCs, type, and status.
type Network struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Name      string              `bson:"name"`
	ChainID   int64               `bson:"chain_id"`
	Type      NetworkType         `bson:"type"`
	RPCURLs   []string            `bson:"rpc_urls"`
	Status    NetworkStatus       `bson:"status"`
	CreatedAt time.Time           `bson:"created_at"`
	UpdatedAt time.Time           `bson:"updated_at"`
}

// CollectionName returns the MongoDB collection name.
func (Network) CollectionName() string {
	return "networks"
}
