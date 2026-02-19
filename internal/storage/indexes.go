package storage

import (
	"context"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// EnsureIndexes creates all indexes defined in SCHEMA_DESIGN.md.
// Idempotent: safe to call multiple times.
func (m *MongoDB) EnsureIndexes(ctx context.Context) error {
	if err := m.ensureNetworkIndexes(ctx); err != nil {
		return err
	}
	if err := m.ensureContractIndexes(ctx); err != nil {
		return err
	}
	if err := m.ensureLogIndexes(ctx); err != nil {
		return err
	}
	return nil
}

func (m *MongoDB) createIndexIfNotExists(ctx context.Context, coll *mongo.Collection, model mongo.IndexModel) error {
	_, err := coll.Indexes().CreateOne(ctx, model)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func (m *MongoDB) ensureNetworkIndexes(ctx context.Context) error {
	coll := m.database.Collection(models.Network{}.CollectionName())
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "chain_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
	} {
		if err := m.createIndexIfNotExists(ctx, coll, model); err != nil {
			return err
		}
	}
	return nil
}

func (m *MongoDB) ensureContractIndexes(ctx context.Context) error {
	coll := m.database.Collection(models.Contract{}.CollectionName())
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "address", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}}},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
	} {
		if err := m.createIndexIfNotExists(ctx, coll, model); err != nil {
			return err
		}
	}
	return nil
}

func (m *MongoDB) ensureLogIndexes(ctx context.Context) error {
	coll := m.database.Collection(models.Log{}.CollectionName())
	for _, model := range []mongo.IndexModel{
		{Keys: bson.D{{Key: "network_id", Value: 1}, {Key: "contract_id", Value: 1}, {Key: "block_number", Value: 1}, {Key: "log_index", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "network_id", Value: 1}}},
		{Keys: bson.D{{Key: "contract_id", Value: 1}}},
		{Keys: bson.D{{Key: "block_number", Value: 1}}},
		{Keys: bson.D{{Key: "transaction_hash", Value: 1}}},
		{Keys: bson.D{{Key: "address", Value: 1}, {Key: "event_signature", Value: 1}}},
	} {
		if err := m.createIndexIfNotExists(ctx, coll, model); err != nil {
			return err
		}
	}
	return nil
}
