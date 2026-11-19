package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SolanaEventConfig maps a discriminator to an event name for a contract (program).
type SolanaEventConfig struct {
	ContractID    int64
	Address       string
	IDL           json.RawMessage
	EventName     string
	Discriminator string
}

// SolanaRuntimeConfig holds everything the Solana indexer needs at startup.
type SolanaRuntimeConfig struct {
	NetworkID         int64
	ChainID           int64
	RPCURLs           []string
	Checkpoint        uint64
	ScanEnabled       bool
	ConfirmationDepth int
	EventConfigs      []SolanaEventConfig
	// Derived lookups
	ProgramAddresses         []string
	ContractIDByAddr         map[string]int64
	IDLByAddr                map[string]json.RawMessage
	DiscriminatorToEventByAddr map[string]map[string]string // addr -> disc -> eventName
}

func (s *Store) LoadSolanaRuntimeConfig(ctx context.Context) (*SolanaRuntimeConfig, error) {
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
	// Solana has negligible reorg risk with confirmed commitment; default depth=0
	confirmationDepth, _ := s.GetConfirmationDepth(ctx, net.ID)

	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.address, c.abi, e.event_name, e.discriminator
		FROM contracts c
		INNER JOIN solana_event_configs e ON e.contract_id = c.id
		WHERE c.network_id = $1 AND c.status = 'active' AND e.status = 'active'`,
		net.ID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cfg := &SolanaRuntimeConfig{
		NetworkID:                  net.ID,
		ChainID:                   net.ChainID,
		RPCURLs:                   net.RPCURLs,
		Checkpoint:                checkpoint,
		ScanEnabled:               scanEnabled,
		ConfirmationDepth:         confirmationDepth,
		ContractIDByAddr:          make(map[string]int64),
		IDLByAddr:                 make(map[string]json.RawMessage),
		DiscriminatorToEventByAddr: make(map[string]map[string]string),
	}
	addrSet := make(map[string]struct{})

	for rows.Next() {
		var ev SolanaEventConfig
		if err := rows.Scan(&ev.ContractID, &ev.Address, &ev.IDL, &ev.EventName, &ev.Discriminator); err != nil {
			return nil, err
		}
		cfg.EventConfigs = append(cfg.EventConfigs, ev)
		cfg.ContractIDByAddr[ev.Address] = ev.ContractID
		if len(ev.IDL) > 0 {
			cfg.IDLByAddr[ev.Address] = ev.IDL
		}
		if _, ok := cfg.DiscriminatorToEventByAddr[ev.Address]; !ok {
			cfg.DiscriminatorToEventByAddr[ev.Address] = make(map[string]string)
		}
		cfg.DiscriminatorToEventByAddr[ev.Address][ev.Discriminator] = ev.EventName
		addrSet[ev.Address] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for addr := range addrSet {
		cfg.ProgramAddresses = append(cfg.ProgramAddresses, addr)
	}
	return cfg, nil
}

// SolLogInsert holds one Solana log row ready for insertion.
type SolLogInsert struct {
	ContractID     int64
	ProgramAddress string
	Slot           uint64
	BlockHash      string
	BlockTimestamp time.Time
	TxHash         string
	LogIndex       int
	Discriminator  string
	EventKey       string
	EventName      string
	RawData        string
	DecodedParams  map[string]any
	AccountKeys    []string
}

// PersistSolanaBlock saves a block + sol_logs in one transaction and advances the checkpoint.
func (s *Store) PersistSolanaBlock(
	ctx context.Context,
	networkID, chainID int64,
	slot uint64,
	blockHash string,
	blockTimestamp time.Time,
	logs []SolLogInsert,
	enqueueOutbox bool,
	nextCheckpoint uint64,
) ([]int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	blockID, err := upsertBlockTx(ctx, tx, networkID, chainID, slot, blockHash, "", blockTimestamp, "confirmed")
	if err != nil {
		return nil, err
	}

	var logIDs []int64
	for _, l := range logs {
		accountKeysJSON, err := json.Marshal(l.AccountKeys)
		if err != nil {
			return nil, err
		}
		var decodedJSON []byte
		if l.DecodedParams != nil {
			decodedJSON, err = json.Marshal(l.DecodedParams)
			if err != nil {
				return nil, err
			}
		}

		var logID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO sol_logs (
				block_id, network_id, chain_id, contract_id, program_address,
				slot, block_hash, block_timestamp, transaction_hash,
				log_index, discriminator, event_key, event_name,
				raw_data, decoded_params, account_keys, status
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb, $16::jsonb, 'confirmed'
			)
			ON CONFLICT (chain_id, slot, transaction_hash, log_index) DO NOTHING
			RETURNING id`,
			blockID, networkID, chainID, l.ContractID, l.ProgramAddress,
			l.Slot, l.BlockHash, l.BlockTimestamp, l.TxHash,
			l.LogIndex, l.Discriminator, l.EventKey, l.EventName,
			l.RawData, decodedJSON, accountKeysJSON,
		).Scan(&logID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue // duplicate
			}
			return nil, fmt.Errorf("insert sol_log: %w", err)
		}
		logIDs = append(logIDs, logID)

		if enqueueOutbox {
			if err := s.enqueueSolOutboxTx(ctx, tx, logID, networkID); err != nil {
				return nil, fmt.Errorf("enqueue sol outbox: %w", err)
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE indexer_config SET value = $1, updated_at = now()
		WHERE network_id = $2 AND key = $3`,
		fmt.Sprintf("%d", nextCheckpoint), networkID, ConfigKeyCheckpoint,
	); err != nil {
		return nil, fmt.Errorf("update checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return logIDs, nil
}

// PersistSolanaCatchupBatch saves logs across a slot range.
func (s *Store) PersistSolanaCatchupBatch(
	ctx context.Context,
	networkID, chainID int64,
	logs []SolLogInsert,
	enqueueOutbox bool,
	nextCheckpoint uint64,
) ([]int64, error) {
	if len(logs) == 0 {
		if err := s.SetCheckpoint(ctx, networkID, nextCheckpoint); err != nil {
			return nil, err
		}
		return nil, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var logIDs []int64
	blockCache := make(map[string]int64)

	for _, l := range logs {
		blockID, ok := blockCache[l.BlockHash]
		if !ok {
			blockID, err = upsertBlockTx(ctx, tx, networkID, chainID, l.Slot, l.BlockHash, "", l.BlockTimestamp, "confirmed")
			if err != nil {
				return nil, err
			}
			blockCache[l.BlockHash] = blockID
		}

		accountKeysJSON, err := json.Marshal(l.AccountKeys)
		if err != nil {
			return nil, err
		}
		var decodedJSON []byte
		if l.DecodedParams != nil {
			decodedJSON, err = json.Marshal(l.DecodedParams)
			if err != nil {
				return nil, err
			}
		}

		var logID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO sol_logs (
				block_id, network_id, chain_id, contract_id, program_address,
				slot, block_hash, block_timestamp, transaction_hash,
				log_index, discriminator, event_key, event_name,
				raw_data, decoded_params, account_keys, status
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb, $16::jsonb, 'confirmed'
			)
			ON CONFLICT (chain_id, slot, transaction_hash, log_index) DO NOTHING
			RETURNING id`,
			blockID, networkID, chainID, l.ContractID, l.ProgramAddress,
			l.Slot, l.BlockHash, l.BlockTimestamp, l.TxHash,
			l.LogIndex, l.Discriminator, l.EventKey, l.EventName,
			l.RawData, decodedJSON, accountKeysJSON,
		).Scan(&logID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, err
		}
		logIDs = append(logIDs, logID)

		if enqueueOutbox {
			if err := s.enqueueSolOutboxTx(ctx, tx, logID, networkID); err != nil {
				return nil, fmt.Errorf("enqueue sol outbox: %w", err)
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE indexer_config SET value = $1, updated_at = now()
		WHERE network_id = $2 AND key = $3`,
		fmt.Sprintf("%d", nextCheckpoint), networkID, ConfigKeyCheckpoint,
	); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return logIDs, nil
}

// BulkInsertSolanaEventConfigs inserts event discriminator configs for a Solana program.
func (s *Store) BulkInsertSolanaEventConfigs(ctx context.Context, contractID int64, rows []SolanaEventConfigInput) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`
			INSERT INTO solana_event_configs (contract_id, event_name, discriminator)
			VALUES ($1, $2, $3)
			ON CONFLICT (contract_id, discriminator) DO NOTHING`,
			contractID, r.EventName, r.Discriminator)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

// SolanaEventConfigInput is input for BulkInsertSolanaEventConfigs.
type SolanaEventConfigInput struct {
	EventName     string
	Discriminator string
}

func (s *Store) enqueueSolOutboxTx(ctx context.Context, tx pgx.Tx, logID, networkID int64) error {
	var p struct {
		LogID           int64
		ChainID         int64
		NetworkName     string
		ContractID      int64
		ContractName    string
		ProgramAddress  string
		EventName       string
		Slot            uint64
		BlockHash       string
		BlockTimestamp  time.Time
		TxHash          string
		LogIndex        int
		Discriminator   string
		EventKey        string
		RawData         string
		DecodedParams   json.RawMessage
		AccountKeys     json.RawMessage
	}

	err := tx.QueryRow(ctx, `
		SELECT l.id, l.chain_id, n.name, l.contract_id, c.name, l.program_address,
			COALESCE(l.event_name, ''), l.slot, l.block_hash, l.block_timestamp,
			l.transaction_hash, l.log_index, l.discriminator, l.event_key,
			l.raw_data, l.decoded_params, l.account_keys
		FROM sol_logs l
		JOIN networks n ON n.id = l.network_id
		JOIN contracts c ON c.id = l.contract_id
		WHERE l.id = $1`, logID,
	).Scan(
		&p.LogID, &p.ChainID, &p.NetworkName, &p.ContractID, &p.ContractName, &p.ProgramAddress,
		&p.EventName, &p.Slot, &p.BlockHash, &p.BlockTimestamp,
		&p.TxHash, &p.LogIndex, &p.Discriminator, &p.EventKey,
		&p.RawData, &p.DecodedParams, &p.AccountKeys,
	)
	if err != nil {
		return fmt.Errorf("load sol outbox payload for log %d: %w", logID, err)
	}
	if p.ContractName == "" {
		p.ContractName = p.ProgramAddress
	}

	topic := strings.ToLower(fmt.Sprintf("%s.%s.%d", p.ContractName, p.EventName, networkID))
	payload := map[string]interface{}{
		"topic":           topic,
		"network_id":      networkID,
		"network_name":    p.NetworkName,
		"chain_id":        p.ChainID,
		"contract_id":     p.ContractID,
		"contract_name":   p.ContractName,
		"program_address": p.ProgramAddress,
		"event_name":      p.EventName,
		"slot":            p.Slot,
		"block_hash":      p.BlockHash,
		"block_timestamp": p.BlockTimestamp,
		"transaction_hash": p.TxHash,
		"log_index":       p.LogIndex,
		"discriminator":   p.Discriminator,
		"event_key":       p.EventKey,
		"raw_data":        p.RawData,
	}
	if len(p.DecodedParams) > 0 {
		var decoded map[string]any
		if err := json.Unmarshal(p.DecodedParams, &decoded); err == nil {
			payload["decoded_params"] = decoded
		}
	}
	if len(p.AccountKeys) > 0 {
		var keys []string
		if err := json.Unmarshal(p.AccountKeys, &keys); err == nil {
			payload["account_keys"] = keys
		}
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (log_id, log_source, status, topic, payload)
		VALUES ($1, 'sol', 'pending', $2, $3::jsonb)
		ON CONFLICT (log_id, log_source) DO UPDATE SET
			status = 'pending',
			topic = EXCLUDED.topic,
			payload = EXCLUDED.payload,
			attempts = 0,
			next_retry_at = now(),
			locked_at = NULL,
			locked_by = NULL,
			last_error = NULL,
			delivered_at = NULL
		WHERE outbox.status = 'cancelled'`,
		logID, topic, payloadJSON,
	)
	return err
}
