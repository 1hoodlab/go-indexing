-- +goose Up
CREATE TABLE webhook_endpoints (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL,
    secret      TEXT,
    headers     JSONB NOT NULL DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE webhook_deliveries (
    id              BIGSERIAL PRIMARY KEY,
    outbox_id       BIGINT NOT NULL REFERENCES outbox (id) ON DELETE CASCADE,
    endpoint_id     BIGINT NOT NULL REFERENCES webhook_endpoints (id),
    http_status     INT,
    response_body   TEXT,
    error_message   TEXT,
    duration_ms     INT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_webhook_deliveries_outbox ON webhook_deliveries (outbox_id);
CREATE INDEX idx_webhook_deliveries_attempted ON webhook_deliveries (attempted_at DESC);

-- +goose Down
DROP TABLE IF EXISTS webhook_deliveries;
DROP TABLE IF EXISTS webhook_endpoints;
