-- +goose Up
CREATE TABLE contracts (
    id          BIGSERIAL PRIMARY KEY,
    network_id  BIGINT NOT NULL REFERENCES networks (id) ON DELETE CASCADE,
    name        TEXT NOT NULL DEFAULT '',
    address     TEXT NOT NULL,
    abi         JSONB,
    status      contract_status NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT contracts_network_address_unique UNIQUE (network_id, address),
    CONSTRAINT contracts_address_format CHECK (address ~ '^0x[0-9a-f]{40}$')
);
CREATE INDEX idx_contracts_network ON contracts (network_id);
CREATE INDEX idx_contracts_status ON contracts (network_id, status);

-- +goose Down
DROP TABLE IF EXISTS contracts;
