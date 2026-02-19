package evm

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Client wraps an Ethereum RPC client for block/transaction queries.
type Client struct {
	*ethclient.Client
	rpcURL string
}

// NewClient creates a new EVM client.
func NewClient(rpcURL string) (*Client, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client: client,
		rpcURL: rpcURL,
	}, nil
}

// BlockByNumber fetches a block by number.
func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return c.Client.BlockByNumber(ctx, number)
}

// HeaderByNumber fetches block header by number.
func (c *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.Client.HeaderByNumber(ctx, number)
}
