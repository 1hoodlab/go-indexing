-- +goose Up
-- parent_hash + UNIQUE (network_id, block_hash) already identifies the parent
-- block unambiguously; parent_block_id was a best-effort cache that stayed NULL
-- whenever the parent block was not indexed (catch-up only stores blocks that
-- have matching logs). Nothing reads it, so drop it.
ALTER TABLE blocks DROP COLUMN parent_block_id;

-- +goose Down
ALTER TABLE blocks ADD COLUMN parent_block_id BIGINT REFERENCES blocks (id);
