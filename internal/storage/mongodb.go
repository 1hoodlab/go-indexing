package storage

import (
	"context"
	"fmt"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB wraps a MongoDB connection for block/transaction storage.
type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewMongoDB creates a new MongoDB connection.
func NewMongoDB(cfg *config.StorageConfig) (*MongoDB, error) {
	uri := cfg.URI
	if uri == "" {
		port := cfg.Port
		if port == 0 {
			port = 27017
		}
		if cfg.User != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
				cfg.User, cfg.Password, cfg.Host, port, cfg.Database)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%d/%s", cfg.Host, port, cfg.Database)
		}
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}

	return &MongoDB{
		client:   client,
		database: client.Database(cfg.Database),
	}, nil
}

// Database returns the database for direct collection access.
func (m *MongoDB) Database() *mongo.Database {
	return m.database
}

// Close closes the MongoDB connection.
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
