package storage

import (
	"context"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/models"
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
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.URI))
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

// Networks returns the networks collection.
func (m *MongoDB) Networks() *mongo.Collection {
	return m.database.Collection(models.Network{}.CollectionName())
}

// Contracts returns the contracts collection.
func (m *MongoDB) Contracts() *mongo.Collection {
	return m.database.Collection(models.Contract{}.CollectionName())
}

// Logs returns the logs collection.
func (m *MongoDB) Logs() *mongo.Collection {
	return m.database.Collection(models.Log{}.CollectionName())
}

// Close closes the MongoDB connection.
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
