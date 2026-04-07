package solana

import (
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Program is a Solana program to index (equivalent to EVM Contract).
type Program struct {
	ID              primitive.ObjectID   `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID   `bson:"network_id"`
	Address         string               `bson:"address"`          // base58 pubkey
	IDL             string               `bson:"idl,omitempty"`    // Anchor IDL JSON
	Events          []string             `bson:"events"`           // event names to watch
	StartSlot       uint64               `bson:"start_slot"`
	LastScannedSlot uint64               `bson:"last_scanned_slot"`
	Status          models.NetworkStatus `bson:"status"`
	CreatedAt       time.Time            `bson:"created_at"`
	UpdatedAt       time.Time            `bson:"updated_at"`
}

// ProgramLog is a raw captured program event (equivalent to EVM Log).
type ProgramLog struct {
	ID              primitive.ObjectID `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID `bson:"network_id"`
	ProgramID       primitive.ObjectID `bson:"program_id"`
	Slot            uint64             `bson:"slot"`
	BlockHash       string             `bson:"block_hash"`
	TransactionHash string             `bson:"transaction_hash"` // base58 signature
	LogIndex        int                `bson:"log_index"`
	ProgramAddress  string             `bson:"program_address"` // base58 pubkey
	Discriminator   string             `bson:"discriminator"`   // hex of first 8 bytes
	EventKey        string             `bson:"event_key"`       // "programAddress:discriminatorHex"
	RawData         string             `bson:"raw_data"`        // base64 original bytes
	AccountKeys     []string           `bson:"account_keys"`    // all accounts in tx
	CreatedAt       time.Time          `bson:"created_at"`
}

// DecodedEvent stores a decoded program event.
type DecodedEvent struct {
	ID              primitive.ObjectID     `bson:"_id,omitempty"`
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ProgramAddress  string                 `bson:"program_address"`
	EventName       string                 `bson:"event_name"` // from IDL, or discriminatorHex
	Slot            uint64                 `bson:"slot"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        int                    `bson:"log_index"`
	Data            map[string]interface{} `bson:"data"`
	CreatedAt       time.Time              `bson:"created_at"`
}
