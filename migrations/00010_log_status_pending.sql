-- +goose NO TRANSACTION
-- +goose Up
-- PostgreSQL forbids using a new enum value in the same transaction as ADD VALUE.
ALTER TYPE log_status ADD VALUE IF NOT EXISTS 'pending' BEFORE 'confirmed';

ALTER TABLE eth_logs ALTER COLUMN status SET DEFAULT 'pending';

-- Align existing rows: logs on pending blocks should be pending too.
UPDATE eth_logs l
SET status = 'pending'
FROM blocks b
WHERE l.block_id = b.id
  AND b.status = 'pending'
  AND l.status = 'confirmed';

CREATE INDEX IF NOT EXISTS idx_eth_logs_pending
    ON eth_logs (network_id, block_number)
    WHERE status = 'pending';

-- +goose Down
DROP INDEX IF EXISTS idx_eth_logs_pending;

ALTER TABLE eth_logs ALTER COLUMN status SET DEFAULT 'confirmed';

-- Cannot remove enum value 'pending' in PostgreSQL without recreating the type.
