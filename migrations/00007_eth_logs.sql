-- +goose Up
CREATE TABLE eth_logs (
    id                  BIGSERIAL PRIMARY KEY,
    block_id            BIGINT NOT NULL REFERENCES blocks (id),
    network_id          BIGINT NOT NULL REFERENCES networks (id),
    chain_id            BIGINT NOT NULL,
    contract_id         BIGINT NOT NULL REFERENCES contracts (id),
    contract_address    TEXT NOT NULL,
    block_number        BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    transaction_hash    TEXT NOT NULL,
    transaction_index   INT,
    log_index           INT NOT NULL,
    event_name          TEXT,
    topics              JSONB NOT NULL DEFAULT '[]',
    topic0              TEXT GENERATED ALWAYS AS (topics->>0) STORED,
    data                TEXT NOT NULL DEFAULT '0x',
    decoded_params      JSONB,
    status              log_status NOT NULL DEFAULT 'pending',
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT eth_logs_dedup_unique
        UNIQUE (chain_id, block_number, transaction_hash, log_index),
    CONSTRAINT eth_logs_topics_is_array CHECK (jsonb_typeof(topics) = 'array'),
    CONSTRAINT eth_logs_topics_length CHECK (jsonb_array_length(topics) BETWEEN 0 AND 4),
    CONSTRAINT eth_logs_decoded_params_is_object
        CHECK (decoded_params IS NULL OR jsonb_typeof(decoded_params) = 'object'),
    CONSTRAINT eth_logs_contract_address_format CHECK (contract_address ~ '^0x[0-9a-f]{40}$')
);
CREATE INDEX idx_eth_logs_block ON eth_logs (block_id);
CREATE INDEX idx_eth_logs_network ON eth_logs (network_id, indexed_at DESC);
CREATE INDEX idx_eth_logs_contract_event ON eth_logs (contract_address, topic0, block_number DESC);
CREATE INDEX idx_eth_logs_block_number ON eth_logs (chain_id, block_number DESC);
CREATE INDEX idx_eth_logs_tx ON eth_logs (transaction_hash);
CREATE INDEX idx_eth_logs_status ON eth_logs (status) WHERE status = 'confirmed';
CREATE INDEX idx_eth_logs_pending ON eth_logs (network_id, block_number) WHERE status = 'pending';
CREATE INDEX idx_eth_logs_topics_gin ON eth_logs USING GIN (topics jsonb_path_ops);
CREATE INDEX idx_eth_logs_decoded_params_gin ON eth_logs USING GIN (decoded_params jsonb_path_ops);

-- +goose Down
DROP TABLE IF EXISTS eth_logs;
