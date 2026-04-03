package handlers

import (
	"context"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// decodedEvent mirrors evm.DecodedEvent but lives here to avoid import cycle.
type decodedEvent struct {
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ContractAddress string                 `bson:"contract_address"`
	EventName       string                 `bson:"event_name"`
	BlockNumber     uint64                 `bson:"block_number"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        uint32                 `bson:"log_index"`
	Data            map[string]interface{} `bson:"data"`
	CreatedAt       time.Time              `bson:"created_at"`
}

// SaveToLogs creates the default handler that:
// 1. Saves raw logs to the logs collection
// 2. Decodes events via ABI and saves to decoded_events collection
func SaveToLogs(store core.StorageInterface) Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		// 1. Save raw logs
		if err := store.SaveEvents(ctx, batch); err != nil {
			return err
		}

		// 2. Decode and save decoded events
		now := time.Now()
		var decoded []interface{}
		for _, ev := range batch {
			abiJSON := ev.Metadata["abi"]
			if abiJSON == "" {
				continue
			}

			eventName := utils.FindEventNameByTopic0(abiJSON, ev.EventKey)
			if eventName == "" {
				continue
			}

			// Reconstruct full topics for decoding
			fullTopics := []string{ev.EventKey}
			if t := ev.Metadata["topics"]; t != "" {
				for _, topic := range strings.Split(t, ",") {
					if topic != "" {
						fullTopics = append(fullTopics, topic)
					}
				}
			}

			data := ev.Metadata["data"]
			if data == "" {
				data = "0x"
			}

			params, err := utils.DecodeEventLog(abiJSON, eventName, fullTopics, data)
			if err != nil {
				log.Printf("decode %s: %v", eventName, err)
				continue
			}

			// Convert *big.Int to strings for BSON
			for k, v := range params {
				if bi, ok := v.(*big.Int); ok && bi != nil {
					params[k] = bi.String()
				}
			}

			networkID, _ := primitive.ObjectIDFromHex(ev.Metadata["network_id"])
			logIndex, _ := strconv.ParseUint(ev.Metadata["log_index"], 10, 32)

			decoded = append(decoded, decodedEvent{
				NetworkID:       networkID,
				ContractAddress: ev.Address,
				EventName:       eventName,
				BlockNumber:     ev.BlockNumber,
				TransactionHash: ev.TransactionHash,
				LogIndex:        uint32(logIndex),
				Data:            params,
				CreatedAt:       now,
			})
		}

		if len(decoded) > 0 {
			if err := store.SaveDecodedEvents(ctx, decoded); err != nil {
				log.Printf("save decoded events: %v", err)
				// Don't return error — raw logs are already saved
			}
		}

		return nil
	}
}
