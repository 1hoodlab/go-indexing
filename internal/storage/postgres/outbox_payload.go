package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxWebhookPayload is the JSON body sent to webhook consumers.
type OutboxWebhookPayload struct {
	Topic           string              `json:"topic"`
	NetworkID       int64               `json:"network_id"`
	NetworkName     string              `json:"network_name"`
	ChainID         int64               `json:"chain_id"`
	ContractID      int64               `json:"contract_id"`
	ContractName    string              `json:"contract_name"`
	ContractAddress string              `json:"contract_address"`
	EventName       string              `json:"event_name"`
	Block           OutboxBlockPayload  `json:"block"`
	Log             OutboxLogPayload    `json:"log"`
}

type OutboxBlockPayload struct {
	ID         int64      `json:"id"`
	Number     uint64     `json:"number"`
	Hash       string     `json:"hash"`
	ParentHash string     `json:"parent_hash,omitempty"`
	Timestamp  time.Time  `json:"timestamp"`
	Status     string     `json:"status"`
	ConfirmedAt *time.Time `json:"confirmed_at,omitempty"`
}

type OutboxLogPayload struct {
	ID               int64          `json:"id"`
	TransactionHash  string         `json:"transaction_hash"`
	TransactionIndex *int           `json:"transaction_index,omitempty"`
	LogIndex         int            `json:"log_index"`
	Topics           []string       `json:"topics"`
	Topic0           string         `json:"topic0,omitempty"`
	Data             string         `json:"data"`
	DecodedParams    map[string]any `json:"decoded_params,omitempty"`
}

func outboxTopic(contractName, eventName string, networkID int64) string {
	return strings.ToLower(fmt.Sprintf("%s.%s.%d", contractName, eventName, networkID))
}

func (s *Store) buildOutboxPayloadTx(ctx context.Context, tx pgx.Tx, logID int64) (topic string, payloadJSON []byte, err error) {
	var p OutboxWebhookPayload
	var topicsJSON []byte
	var decodedJSON []byte
	var txIndex *int
	var confirmedAt *time.Time

	err = tx.QueryRow(ctx, `
		SELECT l.id, l.chain_id, l.network_id, n.name,
			l.contract_id, c.name, l.contract_address, COALESCE(l.event_name, ''),
			b.id, l.block_number, l.block_hash, COALESCE(b.parent_hash, ''),
			l.block_timestamp, b.status::text, b.confirmed_at,
			l.transaction_hash, l.transaction_index, l.log_index,
			l.topics, COALESCE(l.topic0, ''), l.data, l.decoded_params
		FROM eth_logs l
		JOIN networks n ON n.id = l.network_id
		JOIN contracts c ON c.id = l.contract_id
		JOIN blocks b ON b.id = l.block_id
		WHERE l.id = $1`, logID,
	).Scan(
		&p.Log.ID, &p.ChainID, &p.NetworkID, &p.NetworkName,
		&p.ContractID, &p.ContractName, &p.ContractAddress, &p.EventName,
		&p.Block.ID, &p.Block.Number, &p.Block.Hash, &p.Block.ParentHash,
		&p.Block.Timestamp, &p.Block.Status, &confirmedAt,
		&p.Log.TransactionHash, &txIndex, &p.Log.LogIndex,
		&topicsJSON, &p.Log.Topic0, &p.Log.Data, &decodedJSON,
	)
	if err != nil {
		return "", nil, fmt.Errorf("load outbox payload for log %d: %w", logID, err)
	}
	p.Block.ConfirmedAt = confirmedAt
	if txIndex != nil {
		p.Log.TransactionIndex = txIndex
	}
	if len(topicsJSON) > 0 {
		if err := json.Unmarshal(topicsJSON, &p.Log.Topics); err != nil {
			return "", nil, err
		}
	}
	if len(decodedJSON) > 0 {
		if err := json.Unmarshal(decodedJSON, &p.Log.DecodedParams); err != nil {
			return "", nil, err
		}
	}
	if p.ContractName == "" {
		p.ContractName = p.ContractAddress
	}
	p.Topic = outboxTopic(p.ContractName, p.EventName, p.NetworkID)
	topic = p.Topic
	payloadJSON, err = json.Marshal(p)
	if err != nil {
		return "", nil, err
	}
	return topic, payloadJSON, nil
}

func (s *Store) enqueueOutboxTx(ctx context.Context, tx pgx.Tx, logID int64) error {
	topic, payloadJSON, err := s.buildOutboxPayloadTx(ctx, tx, logID)
	if err != nil {
		return err
	}
	// A cancelled row (from a reorg) is revived so the canonical version of the
	// event gets delivered; pending/processing/delivered rows are left untouched.
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (log_id, log_source, status, topic, payload)
		VALUES ($1, 'eth', 'pending', $2, $3::jsonb)
		ON CONFLICT (log_id, log_source) DO UPDATE SET
			status = 'pending',
			topic = EXCLUDED.topic,
			payload = EXCLUDED.payload,
			attempts = 0,
			next_retry_at = now(),
			locked_at = NULL,
			locked_by = NULL,
			last_error = NULL,
			delivered_at = NULL
		WHERE outbox.status = 'cancelled'`,
		logID, topic, payloadJSON,
	)
	return err
}

func (s *Store) BuildOutboxPayload(ctx context.Context, logID int64) (*OutboxWebhookPayload, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	_, payloadJSON, err := s.buildOutboxPayloadTx(ctx, tx, logID)
	if err != nil {
		return nil, err
	}
	var p OutboxWebhookPayload
	if err := json.Unmarshal(payloadJSON, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
