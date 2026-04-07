package solana

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// decodedEventDoc is the document shape written to decoded_events.
type decodedEventDoc struct {
	NetworkID       primitive.ObjectID     `bson:"network_id"`
	ProgramAddress  string                 `bson:"program_address"`
	EventName       string                 `bson:"event_name"`
	Slot            uint64                 `bson:"slot"`
	TransactionHash string                 `bson:"transaction_hash"`
	LogIndex        int                    `bson:"log_index"`
	Data            map[string]interface{} `bson:"data"`
	CreatedAt       time.Time              `bson:"created_at"`
}

// DefaultHandler returns a core.Handler that:
//  1. Saves raw events to program_logs
//  2. Decodes each event via the EventParser
//  3. Saves decoded events to decoded_events
func DefaultHandler(store *Storage, parser *EventParser) core.Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		// 1. Save raw logs.
		if err := store.SaveEvents(ctx, batch); err != nil {
			return err
		}

		// 2. Decode and save.
		now := time.Now()
		var decoded []interface{}
		for _, ev := range batch {
			fields, err := parser.ParseEvent(ev)
			if err != nil {
				log.Printf("solana handler: parse event: %v", err)
				continue
			}
			if fields == nil {
				continue
			}

			eventName, _ := fields["_event_name"].(string)
			if eventName == "" {
				// Raw fallback; use discriminator as name.
				eventName, _ = fields["discriminator"].(string)
			}

			networkID, _ := primitive.ObjectIDFromHex(ev.Metadata["network_id"])
			logIndex, _ := strconv.Atoi(ev.Metadata["log_index"])

			// Remove internal _event_name from the stored data map.
			dataCopy := make(map[string]interface{}, len(fields))
			for k, v := range fields {
				if k != "_event_name" {
					dataCopy[k] = v
				}
			}

			decoded = append(decoded, decodedEventDoc{
				NetworkID:       networkID,
				ProgramAddress:  ev.Address,
				EventName:       eventName,
				Slot:            ev.BlockNumber,
				TransactionHash: ev.TransactionHash,
				LogIndex:        logIndex,
				Data:            dataCopy,
				CreatedAt:       now,
			})
		}

		if len(decoded) > 0 {
			if err := store.SaveDecodedEvents(ctx, decoded); err != nil {
				log.Printf("solana handler: save decoded events: %v", err)
				// Don't return error; raw logs are already saved.
			}
		}

		return nil
	}
}
