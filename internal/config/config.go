package config

import (
	"os"
	"time"
)

type Config struct {
	ChainType    string
	ChainName    string
	RPCURL       string
	PollInterval time.Duration
	MongoURI     string
	Database     string
}

var defaultPollIntervals = map[string]time.Duration{
	"evm":    12 * time.Second,
	"solana": 400 * time.Millisecond,
	"tron":   3 * time.Second,
	"sui":    1 * time.Second,
}

func Load() Config {
	chainType := getenv("CHAIN_TYPE", "evm")

	pollInterval := defaultPollIntervals[chainType]
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			pollInterval = d
		}
	}

	return Config{
		ChainType:    chainType,
		ChainName:    getenv("CHAIN_NAME", chainType),
		RPCURL:       os.Getenv("RPC_URL"),
		PollInterval: pollInterval,
		MongoURI:     getenv("MONGODB_URI", "mongodb://localhost:27017"),
		Database:     getenv("MONGODB_DATABASE", "indexing"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
