-- +goose Up
CREATE TABLE networks (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    chain_id     BIGINT NOT NULL,
    chain_type   TEXT NOT NULL DEFAULT 'evm',
    network_type TEXT NOT NULL DEFAULT 'mainnet',
    rpc_urls     JSONB NOT NULL DEFAULT '[]',
    status       network_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT networks_chain_id_unique UNIQUE (chain_id),
    CONSTRAINT networks_rpc_urls_is_array CHECK (jsonb_typeof(rpc_urls) = 'array')
);
CREATE INDEX idx_networks_status ON networks (status);

-- +goose Down
DROP TABLE IF EXISTS networks;
