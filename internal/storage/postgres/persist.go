package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type BlockRow struct {
	NetworkID    int64
	ChainID      int64
	BlockNumber  uint64
	BlockHash    string
	ParentHash   string
	Timestamp    time.Time
	Status       string
	IsCanonical  bool
}

type LogInsert struct {
	ContractID      int64
	ContractAddress string
	BlockNumber     uint64
	BlockHash       string
	ParentHash      string
	BlockTimestamp  time.Time
	TransactionHash string
	TransactionIndex int
	LogIndex        int
	EventName       string
	Topics          []string
	Data            string
	DecodedParams   map[string]any
}

type PersistBlockResult struct {
	BlockID int64
	LogIDs  []int64
}

func upsertBlockTx(
	ctx context.Context,
	tx pgx.Tx,
	networkID, chainID int64,
	blockNumber uint64,
	blockHash, parentHash string,
	timestamp time.Time,
	status string,
) (int64, error) {
	blockHash = strings.ToLower(blockHash)
	parentHash = strings.ToLower(parentHash)
	var blockID int64
	err := tx.QueryRow(ctx, `
		INSERT INTO blocks (network_id, chain_id, block_number, block_hash, parent_hash,
			block_timestamp, status, is_canonical, confirmed_at, indexed_at)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7::block_status, true,
			CASE WHEN $7::text = 'confirmed' THEN now() ELSE NULL END, now())
		ON CONFLICT (network_id, block_hash) DO UPDATE SET
			status = EXCLUDED.status,
			is_canonical = EXCLUDED.is_canonical,
			block_timestamp = EXCLUDED.block_timestamp,
			parent_hash = COALESCE(NULLIF(EXCLUDED.parent_hash, ''), blocks.parent_hash),
			confirmed_at = CASE
				WHEN EXCLUDED.status = 'confirmed' AND blocks.confirmed_at IS NULL THEN now()
				ELSE blocks.confirmed_at
			END
		RETURNING id`,
		networkID, chainID, blockNumber, blockHash, parentHash,
		timestamp, status,
	).Scan(&blockID)
	if err != nil {
		return 0, fmt.Errorf("upsert block: %w", err)
	}
	if err := linkParentBlockTx(ctx, tx, networkID, blockID); err != nil {
		return 0, err
	}
	return blockID, nil
}

func linkParentBlockTx(ctx context.Context, tx pgx.Tx, networkID, blockID int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE blocks child
		SET parent_block_id = parent.id
		FROM blocks parent
		WHERE child.id = $1
		  AND child.network_id = $2
		  AND child.parent_hash IS NOT NULL
		  AND child.parent_hash = parent.block_hash
		  AND parent.network_id = child.network_id
		  AND child.parent_block_id IS NULL`,
		blockID, networkID,
	)
	return err
}

func linkPendingParentBlocksTx(ctx context.Context, tx pgx.Tx, networkID int64, blockIDs []int64) error {
	for _, blockID := range blockIDs {
		if err := linkParentBlockTx(ctx, tx, networkID, blockID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetBlockByNumber(ctx context.Context, networkID int64, blockNumber uint64) (*BlockRow, error) {
	var b BlockRow
	err := s.pool.QueryRow(ctx, `
		SELECT network_id, chain_id, block_number, block_hash, COALESCE(parent_hash, ''),
			block_timestamp, status::text, is_canonical
		FROM blocks
		WHERE network_id = $1 AND block_number = $2 AND is_canonical = true
		ORDER BY id DESC LIMIT 1`,
		networkID, blockNumber,
	).Scan(&b.NetworkID, &b.ChainID, &b.BlockNumber, &b.BlockHash, &b.ParentHash,
		&b.Timestamp, &b.Status, &b.IsCanonical)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (s *Store) DeleteBlockByHash(ctx context.Context, networkID int64, blockHash string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var blockID int64
	err = tx.QueryRow(ctx, `
		SELECT id FROM blocks WHERE network_id = $1 AND block_hash = $2`,
		networkID, blockHash,
	).Scan(&blockID)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE outbox SET status = 'cancelled'
		WHERE log_id IN (SELECT id FROM eth_logs WHERE block_id = $1)
		  AND status IN ('pending', 'processing')`, blockID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE eth_logs SET status = 'orphaned' WHERE block_id = $1`, blockID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE blocks SET status = 'orphaned', is_canonical = false, orphaned_at = now()
		WHERE id = $1`, blockID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PersistBlock saves block + logs + outbox rows in one transaction when confirmed.
func (s *Store) PersistBlock(
	ctx context.Context,
	networkID, chainID int64,
	block BlockRow,
	logs []LogInsert,
	enqueueOutbox bool,
	nextCheckpoint uint64,
) (*PersistBlockResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	blockStatus := block.Status
	if blockStatus == "" {
		blockStatus = "pending"
	}

	blockID, err := upsertBlockTx(ctx, tx, networkID, chainID, block.BlockNumber, block.BlockHash, block.ParentHash, block.Timestamp, blockStatus)
	if err != nil {
		return nil, err
	}

	result := &PersistBlockResult{BlockID: blockID}
	for _, log := range logs {
		topicsJSON, err := json.Marshal(log.Topics)
		if err != nil {
			return nil, err
		}
		var decodedJSON []byte
		if log.DecodedParams != nil {
			decodedJSON, err = json.Marshal(log.DecodedParams)
			if err != nil {
				return nil, err
			}
		}

		var logID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO eth_logs (
				block_id, network_id, chain_id, contract_id, contract_address,
				block_number, block_hash, block_timestamp, transaction_hash,
				transaction_index, log_index, event_name, topics, data, decoded_params, status
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15::jsonb, $16::log_status
			)
			ON CONFLICT (chain_id, block_number, transaction_hash, log_index) DO NOTHING
			RETURNING id`,
			blockID, networkID, chainID, log.ContractID, log.ContractAddress,
			log.BlockNumber, log.BlockHash, log.BlockTimestamp, log.TransactionHash,
			log.TransactionIndex, log.LogIndex, log.EventName, topicsJSON, log.Data, decodedJSON,
			blockStatus,
		).Scan(&logID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, fmt.Errorf("insert log: %w", err)
		}
		result.LogIDs = append(result.LogIDs, logID)

		if enqueueOutbox {
			if err := s.enqueueOutboxTx(ctx, tx, logID); err != nil {
				return nil, fmt.Errorf("enqueue outbox: %w", err)
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
	return result, nil
}

// PersistCatchupBatch saves logs across a block range and advances checkpoint without per-block rows.
func (s *Store) PersistCatchupBatch(
	ctx context.Context,
	networkID, chainID int64,
	logs []LogInsert,
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
	blockCache := make(map[string]int64) // block_hash -> block_id
	var blockIDs []int64

	for _, log := range logs {
		blockID, ok := blockCache[log.BlockHash]
		if !ok {
			blockID, err = upsertBlockTx(ctx, tx, networkID, chainID, log.BlockNumber, log.BlockHash, log.ParentHash, log.BlockTimestamp, "confirmed")
			if err != nil {
				return nil, err
			}
			blockCache[log.BlockHash] = blockID
			blockIDs = append(blockIDs, blockID)
		}

		topicsJSON, err := json.Marshal(log.Topics)
		if err != nil {
			return nil, err
		}
		var decodedJSON []byte
		if log.DecodedParams != nil {
			decodedJSON, err = json.Marshal(log.DecodedParams)
			if err != nil {
				return nil, err
			}
		}

		var logID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO eth_logs (
				block_id, network_id, chain_id, contract_id, contract_address,
				block_number, block_hash, block_timestamp, transaction_hash,
				transaction_index, log_index, event_name, topics, data, decoded_params, status
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15::jsonb, 'confirmed'
			)
			ON CONFLICT (chain_id, block_number, transaction_hash, log_index) DO NOTHING
			RETURNING id`,
			blockID, networkID, chainID, log.ContractID, log.ContractAddress,
			log.BlockNumber, log.BlockHash, log.BlockTimestamp, log.TransactionHash,
			log.TransactionIndex, log.LogIndex, log.EventName, topicsJSON, log.Data, decodedJSON,
		).Scan(&logID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, err
		}
		logIDs = append(logIDs, logID)

		if enqueueOutbox {
			if err := s.enqueueOutboxTx(ctx, tx, logID); err != nil {
				return nil, fmt.Errorf("enqueue outbox: %w", err)
			}
		}
	}

	if err := linkPendingParentBlocksTx(ctx, tx, networkID, blockIDs); err != nil {
		return nil, err
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
