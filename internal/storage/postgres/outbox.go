package postgres

import (
	"context"
	"encoding/json"
	"time"
)

type OutboxRow struct {
	OutboxID int64
	LogID    int64
	Topic    string
	Payload  []byte
}

type WebhookEndpoint struct {
	ID      int64
	Name    string
	URL     string
	Secret  string
	Enabled bool
}

type LogPayload struct {
	LogID           int64
	ChainID         int64
	NetworkName     string
	BlockNumber     uint64
	BlockHash       string
	BlockTimestamp  time.Time
	TransactionHash string
	LogIndex        int
	ContractAddress string
	EventName       string
	Topics          []string
	Data            string
	DecodedParams   map[string]any
}

func (s *Store) PollOutbox(ctx context.Context, workerID string, limit int) ([]OutboxRow, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, log_id, topic, payload FROM outbox
		WHERE status = 'pending' AND next_retry_at <= now()
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.OutboxID, &r.LogID, &r.Topic, &r.Payload); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, r := range result {
		_, err := tx.Exec(ctx, `
			UPDATE outbox SET status = 'processing', locked_at = now(), locked_by = $1
			WHERE id = $2`, workerID, r.OutboxID)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) LoadLogPayload(ctx context.Context, logID int64) (*LogPayload, error) {
	var p LogPayload
	var topicsJSON []byte
	var decodedJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT l.id, l.chain_id, n.name, l.block_number, l.block_hash, l.block_timestamp,
			l.transaction_hash, l.log_index, l.contract_address, COALESCE(l.event_name, ''),
			l.topics, l.data, l.decoded_params
		FROM eth_logs l
		JOIN networks n ON n.id = l.network_id
		WHERE l.id = $1`, logID,
	).Scan(&p.LogID, &p.ChainID, &p.NetworkName, &p.BlockNumber, &p.BlockHash, &p.BlockTimestamp,
		&p.TransactionHash, &p.LogIndex, &p.ContractAddress, &p.EventName,
		&topicsJSON, &p.Data, &decodedJSON)
	if err != nil {
		return nil, err
	}
	if len(topicsJSON) > 0 {
		if err := json.Unmarshal(topicsJSON, &p.Topics); err != nil {
			return nil, err
		}
	}
	if len(decodedJSON) > 0 {
		if err := json.Unmarshal(decodedJSON, &p.DecodedParams); err != nil {
			return nil, err
		}
	}
	return &p, nil
}

func (s *Store) MarkOutboxDelivered(ctx context.Context, outboxID, endpointID int64, httpStatus int, responseBody string, durationMs int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE outbox SET status = 'delivered', delivered_at = now(), locked_at = NULL, locked_by = NULL
		WHERE id = $1`, outboxID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO webhook_deliveries (outbox_id, endpoint_id, http_status, response_body, duration_ms)
		VALUES ($1, $2, $3, $4, $5)`,
		outboxID, endpointID, httpStatus, responseBody, durationMs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkOutboxFailed(ctx context.Context, outboxID, endpointID int64, errMsg string, httpStatus int, nextRetry time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE outbox SET
			status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed'::outbox_status ELSE 'pending'::outbox_status END,
			attempts = attempts + 1,
			next_retry_at = $2,
			last_error = $3,
			locked_at = NULL,
			locked_by = NULL
		WHERE id = $1`, outboxID, nextRetry, errMsg); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO webhook_deliveries (outbox_id, endpoint_id, http_status, error_message)
		VALUES ($1, $2, $3, $4)`,
		outboxID, endpointID, httpStatus, errMsg); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) LoadEnabledWebhookEndpoints(ctx context.Context) ([]WebhookEndpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, url, COALESCE(secret, ''), enabled
		FROM webhook_endpoints WHERE enabled = true ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var endpoints []WebhookEndpoint
	for rows.Next() {
		var e WebhookEndpoint
		if err := rows.Scan(&e.ID, &e.Name, &e.URL, &e.Secret, &e.Enabled); err != nil {
			return nil, err
		}
		endpoints = append(endpoints, e)
	}
	return endpoints, rows.Err()
}

func (s *Store) UpsertWebhookEndpoint(ctx context.Context, name, url, secret string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM webhook_endpoints WHERE url = $1 LIMIT 1`, url).Scan(&id)
	if err == nil {
		return id, nil
	}
	err = s.pool.QueryRow(ctx, `
		INSERT INTO webhook_endpoints (name, url, secret, enabled, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), true, now())
		RETURNING id`, name, url, secret).Scan(&id)
	return id, err
}
