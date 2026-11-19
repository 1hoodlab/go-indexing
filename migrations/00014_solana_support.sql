-- +goose Up

-- Relax address format to allow both EVM (0x hex) and Solana (base58)
ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_address_format;
ALTER TABLE contracts ADD CONSTRAINT contracts_address_format
    CHECK (address ~ '^(0x[0-9a-f]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})$');

-- Solana event configs (equivalent of evm_event_signatures)
CREATE TABLE solana_event_configs (
    id              BIGSERIAL PRIMARY KEY,
    contract_id     BIGINT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    event_name      TEXT NOT NULL,
    discriminator   TEXT NOT NULL,  -- hex of sha256("event:<Name>")[0:8]
    status          contract_status NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT solana_event_configs_dedup UNIQUE (contract_id, discriminator)
);
CREATE INDEX idx_solana_event_configs_contract ON solana_event_configs (contract_id);

-- Solana logs table (parallel to eth_logs)
CREATE TABLE sol_logs (
    id                  BIGSERIAL PRIMARY KEY,
    block_id            BIGINT NOT NULL REFERENCES blocks (id),
    network_id          BIGINT NOT NULL REFERENCES networks (id),
    chain_id            BIGINT NOT NULL,
    contract_id         BIGINT NOT NULL REFERENCES contracts (id),
    program_address     TEXT NOT NULL,
    slot                BIGINT NOT NULL,
    block_hash          TEXT NOT NULL,
    block_timestamp     TIMESTAMPTZ NOT NULL,
    transaction_hash    TEXT NOT NULL,
    log_index           INT NOT NULL,
    discriminator       TEXT NOT NULL,
    event_key           TEXT NOT NULL,     -- "programAddress:discriminatorHex"
    event_name          TEXT,
    raw_data            TEXT NOT NULL,     -- base64 encoded
    decoded_params      JSONB,
    account_keys        JSONB NOT NULL DEFAULT '[]',
    status              log_status NOT NULL DEFAULT 'pending',
    indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sol_logs_dedup_unique
        UNIQUE (chain_id, slot, transaction_hash, log_index),
    CONSTRAINT sol_logs_decoded_params_is_object
        CHECK (decoded_params IS NULL OR jsonb_typeof(decoded_params) = 'object'),
    CONSTRAINT sol_logs_account_keys_is_array
        CHECK (jsonb_typeof(account_keys) = 'array'),
    CONSTRAINT sol_logs_program_address_format
        CHECK (program_address ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$')
);
CREATE INDEX idx_sol_logs_block ON sol_logs (block_id);
CREATE INDEX idx_sol_logs_network ON sol_logs (network_id, indexed_at DESC);
CREATE INDEX idx_sol_logs_program_event ON sol_logs (program_address, discriminator, slot DESC);
CREATE INDEX idx_sol_logs_slot ON sol_logs (chain_id, slot DESC);
CREATE INDEX idx_sol_logs_tx ON sol_logs (transaction_hash);
CREATE INDEX idx_sol_logs_status ON sol_logs (status) WHERE status = 'confirmed';
CREATE INDEX idx_sol_logs_pending ON sol_logs (network_id, slot) WHERE status = 'pending';
CREATE INDEX idx_sol_logs_decoded_params_gin ON sol_logs USING GIN (decoded_params jsonb_path_ops);

-- Allow outbox to reference both eth_logs and sol_logs
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_log_id_fkey;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS log_source TEXT NOT NULL DEFAULT 'eth';
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_log_unique;
ALTER TABLE outbox ADD CONSTRAINT outbox_log_source_unique UNIQUE (log_id, log_source);

-- +goose Down
ALTER TABLE outbox DROP COLUMN IF EXISTS log_source;
-- Restore original FK (best effort; orphaned sol rows would block this)
-- ALTER TABLE outbox ADD CONSTRAINT outbox_log_id_fkey FOREIGN KEY (log_id) REFERENCES eth_logs (id) ON DELETE RESTRICT;
DROP TABLE IF EXISTS sol_logs;
DROP TABLE IF EXISTS solana_event_configs;
ALTER TABLE contracts DROP CONSTRAINT IF EXISTS contracts_address_format;
ALTER TABLE contracts ADD CONSTRAINT contracts_address_format
    CHECK (address ~ '^0x[0-9a-f]{40}$');
