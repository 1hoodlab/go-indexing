-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE network_status AS ENUM ('active', 'deactive');
CREATE TYPE contract_status AS ENUM ('active', 'deactive');
CREATE TYPE block_status AS ENUM ('pending', 'confirmed', 'orphaned');
CREATE TYPE log_status AS ENUM ('pending', 'confirmed', 'orphaned');
CREATE TYPE outbox_status AS ENUM ('pending', 'processing', 'delivered', 'failed', 'cancelled');

-- +goose Down
DROP TYPE IF EXISTS outbox_status;
DROP TYPE IF EXISTS log_status;
DROP TYPE IF EXISTS block_status;
DROP TYPE IF EXISTS contract_status;
DROP TYPE IF EXISTS network_status;
