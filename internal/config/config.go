package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration.
type Config struct {
	EVM      EVMConfig      `yaml:"evm"`
	Storage  StorageConfig  `yaml:"storage"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

// EVMConfig holds EVM chain connection settings.
type EVMConfig struct {
	RPCURL     string `yaml:"rpc_url"`
	ChainID    int64  `yaml:"chain_id"`
	StartBlock uint64 `yaml:"start_block"`
}

// StorageConfig holds MongoDB connection settings.
type StorageConfig struct {
	URI      string `yaml:"uri"`       // Full connection URI (optional, overrides host/port/user/pass)
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// PipelineConfig holds pipeline processing settings.
type PipelineConfig struct {
	BatchSize    int `yaml:"batch_size"`
	WorkerCount  int `yaml:"worker_count"`
	BufferSize   int `yaml:"buffer_size"`
}

// Load reads and parses the configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Override with env vars if needed
	if uri := os.Getenv("MONGODB_URI"); uri != "" {
		cfg.Storage.URI = uri
	} else if pwd := os.Getenv("MONGODB_PASSWORD"); pwd != "" {
		cfg.Storage.Password = pwd
	}

	return &cfg, nil
}
