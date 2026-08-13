package postgres

import (
	"context"
	"fmt"
)

// ConfirmPendingBlocks promotes blocks and logs up to confirmedHead and enqueues outbox.
func (s *Store) ConfirmPendingBlocks(ctx context.Context, networkID int64, confirmedHead uint64) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var blockCount int
	err = tx.QueryRow(ctx, `
		WITH confirmed_blocks AS (
			UPDATE blocks
			SET status = 'confirmed', confirmed_at = now()
			WHERE network_id = $1
			  AND status = 'pending'
			  AND is_canonical = true
			  AND block_number <= $2
			RETURNING id
		)
		SELECT COUNT(*)::int FROM confirmed_blocks`,
		networkID, confirmedHead,
	).Scan(&blockCount)
	if err != nil {
		return 0, fmt.Errorf("confirm pending blocks: %w", err)
	}

	rows, err := tx.Query(ctx, `
		WITH confirmed_blocks AS (
			SELECT id FROM blocks
			WHERE network_id = $1
			  AND status = 'confirmed'
			  AND is_canonical = true
			  AND block_number <= $2
		)
		UPDATE eth_logs l
		SET status = 'confirmed'
		FROM confirmed_blocks b
		WHERE l.block_id = b.id
		  AND l.status = 'pending'
		RETURNING l.id`,
		networkID, confirmedHead,
	)
	if err != nil {
		return 0, err
	}

	var logIDs []int64
	for rows.Next() {
		var logID int64
		if err := rows.Scan(&logID); err != nil {
			rows.Close()
			return 0, err
		}
		logIDs = append(logIDs, logID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	for _, logID := range logIDs {
		if err := s.enqueueOutboxTx(ctx, tx, logID); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return blockCount, nil
}
