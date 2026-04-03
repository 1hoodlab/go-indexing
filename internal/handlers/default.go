package handlers

import (
	"context"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// SaveToLogs creates the default handler that persists events via StorageInterface.
func SaveToLogs(store core.StorageInterface) Handler {
	return func(ctx context.Context, batch []core.RawEvent) error {
		return store.SaveEvents(ctx, batch)
	}
}
