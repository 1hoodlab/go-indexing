-- +goose Up
UPDATE blocks
SET confirmed_at = indexed_at
WHERE status = 'confirmed' AND confirmed_at IS NULL;

ALTER TABLE blocks DROP COLUMN IF EXISTS confirmation_depth;

-- +goose Down
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS confirmation_depth INT;
