package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	ChainType string
	ChainName string
	PostgresDSN string

	BlockBatchSize            uint64
	TipFollowingEnabled       bool
	RetryAfterReceiptsNull    time.Duration
	SleepAfterCatchupBatch    time.Duration
	NewBlockWaitingTime       time.Duration
	PollIntervalWhenConfirmed time.Duration
	RetryAfterFailed          time.Duration

	WebhookURL    string
	WebhookSecret string
	ConsumerID    string
}

func Load() Config {
	godotenv.Load()

	return Config{
		ChainType:    getenv("CHAIN_TYPE", "evm"),
		ChainName:    getenv("CHAIN_NAME", "evm"),
		PostgresDSN:  getenv("POSTGRES_DSN", "postgres://indexer:indexer@localhost:5432/indexing?sslmode=disable"),

		BlockBatchSize:            uint64(getenvInt("INDEXER_BLOCK_BATCH_SIZE", 100)),
		TipFollowingEnabled:       getenvBool("INDEXER_TIP_FOLLOWING_ENABLED", true),
		RetryAfterReceiptsNull:    getenvDuration("INDEXER_RETRY_AFTER_RECEIPTS_NULL", time.Second),
		SleepAfterCatchupBatch:    getenvDuration("INDEXER_SLEEP_AFTER_CATCHUP_BATCH", time.Second),
		NewBlockWaitingTime:       getenvDuration("INDEXER_NEW_BLOCK_WAITING_TIME", 500*time.Millisecond),
		PollIntervalWhenConfirmed: getenvDuration("INDEXER_POLL_INTERVAL_WHEN_CONFIRMED", 5*time.Second),
		RetryAfterFailed:          getenvDuration("INDEXER_RETRY_AFTER_FAILED", 5*time.Second),

		WebhookURL:    os.Getenv("WEBHOOK_URL"),
		WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
		ConsumerID:    getenv("CONSUMER_ID", "consumer-1"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "1" || v == "true" || v == "TRUE"
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
