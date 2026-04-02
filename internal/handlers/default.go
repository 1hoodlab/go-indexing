package handlers

import (
	"context"
	"log"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/1hoodlab/go-indexing.git/internal/storage"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SaveToLogs creates the default handler that bulk inserts logs into the logs collection.
func SaveToLogs(store *storage.MongoDB) Handler {
	return func(ctx context.Context, batch []*models.Log) error {
		if len(batch) == 0 {
			return nil
		}

		now := time.Now()
		docs := make([]interface{}, len(batch))
		for i, l := range batch {
			l.CreatedAt = now
			docs[i] = l
		}

		opts := options.InsertMany().SetOrdered(false)
		_, err := store.Logs().InsertMany(ctx, docs, opts)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				log.Printf("default handler: skipped %d duplicate logs (block reprocess)", len(batch))
				return nil
			}
			log.Printf("default handler: insert logs failed: %v", err)
			return err
		}

		return nil
	}
}

// SaveToLogsOrdered creates a handler that inserts with ordered=true (stop on first error).
func SaveToLogsOrdered(store *storage.MongoDB) Handler {
	return func(ctx context.Context, batch []*models.Log) error {
		if len(batch) == 0 {
			return nil
		}

		now := time.Now()
		docs := make([]interface{}, len(batch))
		for i, l := range batch {
			l.CreatedAt = now
			docs[i] = l
		}

		_, err := store.Logs().InsertMany(ctx, docs)
		if err != nil {
			return err
		}

		return nil
	}
}
