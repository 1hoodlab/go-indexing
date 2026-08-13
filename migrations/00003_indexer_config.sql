-- +goose Up
CREATE TABLE indexer_config (
    id          BIGSERIAL PRIMARY KEY,
    network_id  BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT indexer_config_network_key_unique UNIQUE (network_id, key)
);
CREATE INDEX idx_indexer_config_network ON indexer_config (network_id);

-- +goose Down
DROP TABLE IF EXISTS indexer_config;
