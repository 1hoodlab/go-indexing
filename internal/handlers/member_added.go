package handlers

import (
	"context"
	"log"
	"math/big"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/1hoodlab/go-indexing.git/internal/storage"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
	"go.mongodb.org/mongo-driver/bson"
)

// MemberAdded creates a handler for MemberAdded(address,uint8,uint256,uint256) events.
// Decodes via ABI from schema, sets log.DataDecoded, then saves to logs collection.
func MemberAdded(store *storage.MongoDB) Handler {
	return func(ctx context.Context, batch []*models.Log) error {
		abiCache := make(map[string]string)

		for _, l := range batch {
			contractIDHex := l.ContractID.Hex()
			abiJSON, ok := abiCache[contractIDHex]
			if !ok {
				var c models.Contract
				if err := store.Contracts().FindOne(ctx, bson.M{"_id": l.ContractID}).Decode(&c); err != nil {
					log.Printf("MemberAdded: fetch contract %s: %v", contractIDHex, err)
					continue
				}
				abiJSON = c.ABI
				abiCache[contractIDHex] = abiJSON
			}

			fullTopics := []string{l.EventSignature}
			fullTopics = append(fullTopics, l.Topics...)

			decoded, err := utils.DecodeEventLog(abiJSON, "MemberAdded", fullTopics, l.Data)
			if err != nil {
				log.Printf("MemberAdded: decode: %v", err)
				continue
			}

			// Structure: { event_name: "MemberAdded", data: { member, role, ... } }
			l.DataDecoded = map[string]interface{}{
				"event_name": "MemberAdded",
				"data":       toBSONMap(decoded),
			}
		}

		return SaveToLogs(store)(ctx, batch)
	}
}

// toBSONMap converts decoded map to BSON-safe values (*big.Int -> string).
func toBSONMap(decoded map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(decoded))
	for k, v := range decoded {
		switch val := v.(type) {
		case *big.Int:
			if val != nil {
				out[k] = val.String()
			} else {
				out[k] = nil
			}
		default:
			out[k] = v
		}
	}
	return out
}
