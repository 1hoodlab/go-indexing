package postgres

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *Store) UpsertContract(ctx context.Context, networkID int64, name, address string, abi json.RawMessage) (int64, error) {
	address = strings.ToLower(address)
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO contracts (network_id, name, address, abi, status, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, 'active', now())
		ON CONFLICT (network_id, address) DO UPDATE SET
			name = EXCLUDED.name,
			abi = EXCLUDED.abi,
			updated_at = now()
		RETURNING id`,
		networkID, name, address, abi,
	).Scan(&id)
	return id, err
}

type EventSignatureInput struct {
	EventName string
	Signature string
	Topic0    string
}

func (s *Store) BulkInsertEventSignatures(ctx context.Context, contractID int64, rows []EventSignatureInput) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`
			INSERT INTO evm_event_signatures (contract_id, event_name, signature, topic0)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (contract_id, topic0) DO NOTHING`,
			contractID, r.EventName, r.Signature, r.Topic0)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}
