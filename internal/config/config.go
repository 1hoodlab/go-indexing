package config

import "os"

// StorageConfig holds MongoDB connection settings (from env).
type StorageConfig struct {
	URI      string
	Database string
}

// MongoDBConfig returns MongoDB config from environment.
func MongoDBConfig() StorageConfig {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	db := os.Getenv("MONGODB_DATABASE")
	if db == "" {
		db = "indexing"
	}
	return StorageConfig{URI: uri, Database: db}
}
