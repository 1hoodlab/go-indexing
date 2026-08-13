-- +goose Up
CREATE TABLE outbox (
    id              BIGSERIAL PRIMARY KEY,
    log_id          BIGINT NOT NULL REFERENCES eth_logs (id) ON DELETE RESTRICT,
    status          outbox_status NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 5,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,
    topic           TEXT NOT NULL DEFAULT '',
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT outbox_log_unique UNIQUE (log_id)
);
CREATE INDEX idx_outbox_pending ON outbox (next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_outbox_processing ON outbox (locked_at) WHERE status = 'processing';

-- +goose Down
DROP TABLE IF EXISTS outbox;
