-- +goose Up
CREATE TABLE evm_event_signatures (
    id           BIGSERIAL PRIMARY KEY,
    contract_id  BIGINT NOT NULL REFERENCES contracts (id) ON DELETE CASCADE,
    event_name   TEXT NOT NULL,
    signature    TEXT NOT NULL,
    topic0       TEXT NOT NULL,
    status       contract_status NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT evm_event_signatures_contract_topic0_unique UNIQUE (contract_id, topic0),
    CONSTRAINT evm_event_signatures_topic0_format CHECK (topic0 ~ '^0x[0-9a-f]{64}$')
);
CREATE INDEX idx_evm_event_signatures_contract ON evm_event_signatures (contract_id);
CREATE INDEX idx_evm_event_signatures_topic0 ON evm_event_signatures (topic0);
CREATE INDEX idx_evm_event_signatures_active
    ON evm_event_signatures (contract_id, status) WHERE status = 'active';

-- +goose Down
DROP TABLE IF EXISTS evm_event_signatures;
