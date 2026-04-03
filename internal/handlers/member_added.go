package handlers

import (
	"context"
	"log"
	"math/big"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

// MemberAdded creates a handler for MemberAdded(address,uint8,uint256,uint256) events.
func MemberAdded(store core.StorageInterface) Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		for i, ev := range batch {
			abiJSON := ev.Metadata["abi"]
			if abiJSON == "" {
				log.Printf("MemberAdded: no ABI for contract %s", ev.Address)
				continue
			}

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

			decoded, err := utils.DecodeEventLog(abiJSON, "MemberAdded", fullTopics, data)
			if err != nil {
				log.Printf("MemberAdded: decode error: %v", err)
				continue
			}

			for k, v := range decoded {
				if bi, ok := v.(*big.Int); ok && bi != nil {
					decoded[k] = bi.String()
				}
			}

			batch[i].Metadata["event_name"] = "MemberAdded"
		}

		return store.SaveEvents(ctx, batch)
	}
}
