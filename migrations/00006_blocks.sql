-- +goose Up
CREATE TABLE blocks (
    id                  BIGSERIAL PRIMARY KEY,
    network_id          BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    chain_id            BIGINT NOT NULL,
    block_number        BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    parent_hash         TEXT,
    parent_block_id     BIGINT REFERENCES blocks (id),
    block_timestamp     TIMESTAMPTZ NOT NULL,
    status              block_status NOT NULL DEFAULT 'pending',
    is_canonical        BOOLEAN NOT NULL DEFAULT true,
    confirmed_at        TIMESTAMPTZ,
    orphaned_at         TIMESTAMPTZ,
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT blocks_network_hash_unique UNIQUE (network_id, block_hash),
    CONSTRAINT blocks_hash_format CHECK (block_hash ~ '^0x[0-9a-f]{64}$')
);
CREATE UNIQUE INDEX idx_blocks_canonical_height
    ON blocks (network_id, block_number) WHERE is_canonical = true;
CREATE INDEX idx_blocks_network_number ON blocks (network_id, block_number DESC);
CREATE INDEX idx_blocks_parent_hash ON blocks (network_id, parent_hash);
CREATE INDEX idx_blocks_status ON blocks (network_id, status);
CREATE INDEX idx_blocks_head ON blocks (network_id, is_canonical, block_number DESC)
    WHERE is_canonical = true;

-- +goose Down
DROP TABLE IF EXISTS blocks;
