package evm

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// Client wraps an Ethereum RPC client for block/transaction queries.
type Client struct {
	*ethclient.Client
	rpc    *rpc.Client
	rpcURL string
}

// NewClient creates a new EVM client.
func NewClient(rpcURL string) (*Client, error) {
	rpcClient, err := rpc.Dial(rpcURL)
	if err != nil {
		return nil, err
	}
	client := ethclient.NewClient(rpcClient)
	return &Client{
		Client: client,
		rpc:    rpcClient,
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

// FilterLogs fetches logs for the given block range and contract addresses.
func (c *Client) FilterLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []common.Address) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		Addresses: addresses,
	}
	return c.Client.FilterLogs(ctx, query)
}

type rpcReceipt struct {
	TransactionHash  common.Hash    `json:"transactionHash"`
	TransactionIndex hexUint        `json:"transactionIndex"`
	Logs             []types.Log    `json:"logs"`
}

type hexUint uint

func (h *hexUint) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 0); !ok {
		return json.Unmarshal(data, &s)
	}
	*h = hexUint(n.Uint64())
	return nil
}

// GetBlockReceipts calls eth_getBlockReceipts.
func (c *Client) GetBlockReceipts(ctx context.Context, blockHash common.Hash) ([]RpcReceipt, error) {
	var raw []rpcReceipt
	err := c.rpc.CallContext(ctx, &raw, "eth_getBlockReceipts", blockHash)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	out := make([]RpcReceipt, len(raw))
	for i, r := range raw {
		out[i] = RpcReceipt{
			TransactionHash:  r.TransactionHash,
			TransactionIndex: uint(r.TransactionIndex),
			Logs:             r.Logs,
		}
	}
	return out, nil
}
