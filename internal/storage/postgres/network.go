package postgres

import (
	"context"
	"encoding/json"
	"fmt"
)

type Network struct {
	ID          int64
	Name        string
	ChainID     int64
	ChainType   string
	NetworkType string
	RPCURLs     []string
	Status      string
}

func (s *Store) LoadActiveNetwork(ctx context.Context) (*Network, error) {
	var n Network
	var rpcJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, chain_id, chain_type, network_type, rpc_urls, status
		FROM networks WHERE status = 'active' ORDER BY id LIMIT 1`,
	).Scan(&n.ID, &n.Name, &n.ChainID, &n.ChainType, &n.NetworkType, &rpcJSON, &n.Status)
	if err != nil {
		return nil, fmt.Errorf("load active network: %w", err)
	}
	if err := json.Unmarshal(rpcJSON, &n.RPCURLs); err != nil {
		return nil, fmt.Errorf("unmarshal rpc_urls: %w", err)
	}
	return &n, nil
}

func (s *Store) UpsertNetwork(ctx context.Context, n Network) (int64, error) {
	rpcJSON, err := json.Marshal(n.RPCURLs)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.pool.QueryRow(ctx, `
		INSERT INTO networks (name, chain_id, chain_type, network_type, rpc_urls, status, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6::network_status, now())
		ON CONFLICT (chain_id) DO UPDATE SET
			name = EXCLUDED.name,
			chain_type = EXCLUDED.chain_type,
			network_type = EXCLUDED.network_type,
			rpc_urls = EXCLUDED.rpc_urls,
			status = EXCLUDED.status,
			updated_at = now()
		RETURNING id`,
		n.Name, n.ChainID, n.ChainType, n.NetworkType, rpcJSON, n.Status,
	).Scan(&id)
	return id, err
}
