package storage

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB wraps a MongoDB connection.
type MongoDB struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewMongoDB creates a new MongoDB connection.
func NewMongoDB(uri, database string) (*MongoDB, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}
	return &MongoDB{
		client:   client,
		database: client.Database(database),
	}, nil
}

// Database returns the database for direct collection access.
func (m *MongoDB) Database() *mongo.Database {
	return m.database
}

// Collection returns a named collection.
func (m *MongoDB) Collection(name string) *mongo.Collection {
	return m.database.Collection(name)
}

// Close closes the MongoDB connection.
func (m *MongoDB) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
