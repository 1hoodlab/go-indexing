package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	ConfigKeyScanEnabled        = "scan_enabled"
	ConfigKeyCheckpoint         = "checkpoint"
	ConfigKeyConfirmationDepth  = "confirmation_depth"
)

func (s *Store) UpsertConfig(ctx context.Context, networkID int64, key, value, description string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO indexer_config (network_id, key, value, description, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (network_id, key) DO UPDATE SET
			value = EXCLUDED.value,
			description = EXCLUDED.description,
			updated_at = now()`,
		networkID, key, value, description,
	)
	return err
}

func (s *Store) GetConfigValue(ctx context.Context, networkID int64, key string) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx, `
		SELECT value FROM indexer_config WHERE network_id = $1 AND key = $2`,
		networkID, key,
	).Scan(&value)
	return value, err
}

func (s *Store) SetCheckpoint(ctx context.Context, networkID int64, block uint64) error {
	return s.UpsertConfig(ctx, networkID, ConfigKeyCheckpoint, strconv.FormatUint(block, 10), "Next block number to scan")
}

func (s *Store) GetCheckpoint(ctx context.Context, networkID int64) (uint64, error) {
	v, err := s.GetConfigValue(ctx, networkID, ConfigKeyCheckpoint)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse checkpoint: %w", err)
	}
	return n, nil
}

func (s *Store) IsScanEnabled(ctx context.Context, networkID int64) (bool, error) {
	v, err := s.GetConfigValue(ctx, networkID, ConfigKeyScanEnabled)
	if err != nil {
		return false, err
	}
	return v == "true", nil
}

func (s *Store) GetConfirmationDepth(ctx context.Context, networkID int64) (int, error) {
	v, err := s.GetConfigValue(ctx, networkID, ConfigKeyConfirmationDepth)
	if err != nil {
		return 12, err
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 12, fmt.Errorf("parse confirmation_depth: %w", err)
	}
	return n, nil
}

type ContractEvent struct {
	ContractID int64
	Address    string
	ABI        json.RawMessage
	EventName  string
	Signature  string
	Topic0     string
}

type IndexerRuntimeConfig struct {
	NetworkID             int64
	ChainID               int64
	RPCURLs               []string
	Checkpoint            uint64
	ScanEnabled           bool
	ConfirmationDepth     int
	ContractEvents        []ContractEvent
	AllowedTopic0ByAddr   map[string]map[string]struct{}
	ABIByAddr             map[string]json.RawMessage
	EventNameByAddrTopic0 map[string]map[string]string
	Addresses             []string
}

func (s *Store) LoadIndexerRuntimeConfig(ctx context.Context) (*IndexerRuntimeConfig, error) {
	net, err := s.LoadActiveNetwork(ctx)
	if err != nil {
		return nil, err
	}

	checkpoint, err := s.GetCheckpoint(ctx, net.ID)
	if err != nil {
		return nil, err
	}
	scanEnabled, err := s.IsScanEnabled(ctx, net.ID)
	if err != nil {
		return nil, err
	}
	confirmationDepth, err := s.GetConfirmationDepth(ctx, net.ID)
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.address, c.abi, e.event_name, e.signature, e.topic0
		FROM contracts c
		INNER JOIN evm_event_signatures e ON e.contract_id = c.id
		WHERE c.network_id = $1 AND c.status = 'active' AND e.status = 'active'`,
		net.ID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cfg := &IndexerRuntimeConfig{
		NetworkID:             net.ID,
		ChainID:               net.ChainID,
		RPCURLs:               net.RPCURLs,
		Checkpoint:            checkpoint,
		ScanEnabled:           scanEnabled,
		ConfirmationDepth:     confirmationDepth,
		AllowedTopic0ByAddr:   make(map[string]map[string]struct{}),
		ABIByAddr:             make(map[string]json.RawMessage),
		EventNameByAddrTopic0: make(map[string]map[string]string),
	}
	addrSet := make(map[string]struct{})

	for rows.Next() {
		var ev ContractEvent
		if err := rows.Scan(&ev.ContractID, &ev.Address, &ev.ABI, &ev.EventName, &ev.Signature, &ev.Topic0); err != nil {
			return nil, err
		}
		cfg.ContractEvents = append(cfg.ContractEvents, ev)

		addr := ev.Address
		topic0 := ev.Topic0
		if _, ok := cfg.AllowedTopic0ByAddr[addr]; !ok {
			cfg.AllowedTopic0ByAddr[addr] = make(map[string]struct{})
		}
		cfg.AllowedTopic0ByAddr[addr][topic0] = struct{}{}

		if len(ev.ABI) > 0 {
			cfg.ABIByAddr[addr] = ev.ABI
		}
		if _, ok := cfg.EventNameByAddrTopic0[addr]; !ok {
			cfg.EventNameByAddrTopic0[addr] = make(map[string]string)
		}
		cfg.EventNameByAddrTopic0[addr][topic0] = ev.EventName

		addrSet[addr] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for addr := range addrSet {
		cfg.Addresses = append(cfg.Addresses, addr)
	}
	return cfg, nil
}
