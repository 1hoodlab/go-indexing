package solana

import (
	"context"

	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// Block is a simplified Solana block for indexing.
type Block struct {
	Slot         uint64
	Blockhash    string
	BlockTime    int64
	Transactions []Transaction
}

// Transaction holds the indexing-relevant fields from a Solana transaction.
type Transaction struct {
	Signature   string
	AccountKeys []string
	LogMessages []string
}

// Client wraps a gagliardetto RPC client with confirmed commitment.
type Client struct {
	rpc *solanarpc.Client
}

func NewClient(url string) (*Client, error) {
	return &Client{rpc: solanarpc.New(url)}, nil
}

func (c *Client) GetSlot(ctx context.Context) (uint64, error) {
	return c.rpc.GetSlot(ctx, solanarpc.CommitmentConfirmed)
}

func (c *Client) GetBlocksWithLimit(ctx context.Context, startSlot, limit uint64) ([]uint64, error) {
	result, err := c.rpc.GetBlocksWithLimit(ctx, startSlot, limit, solanarpc.CommitmentConfirmed)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return []uint64(*result), nil
}

func (c *Client) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	maxVersion := uint64(0)
	noRewards := false

	result, err := c.rpc.GetBlockWithOpts(ctx, slot, &solanarpc.GetBlockOpts{
		Commitment:                     solanarpc.CommitmentConfirmed,
		Encoding:                       solanago.EncodingJSON,
		TransactionDetails:             solanarpc.TransactionDetailsFull,
		MaxSupportedTransactionVersion: &maxVersion,
		Rewards:                        &noRewards,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}

	block := &Block{
		Slot:      slot,
		Blockhash: result.Blockhash.String(),
	}
	if result.BlockTime != nil {
		block.BlockTime = int64(*result.BlockTime)
	}

	for _, txWithMeta := range result.Transactions {
		if txWithMeta.Transaction == nil || txWithMeta.Meta == nil {
			continue
		}
		tx, err := txWithMeta.GetTransaction()
		if err != nil {
			continue
		}
		if len(tx.Signatures) == 0 {
			continue
		}

		var accounts []string
		for _, key := range tx.Message.AccountKeys {
			accounts = append(accounts, key.String())
		}
		for _, key := range txWithMeta.Meta.LoadedAddresses.Writable {
			accounts = append(accounts, key.String())
		}
		for _, key := range txWithMeta.Meta.LoadedAddresses.ReadOnly {
			accounts = append(accounts, key.String())
		}

		block.Transactions = append(block.Transactions, Transaction{
			Signature:   tx.Signatures[0].String(),
			AccountKeys: accounts,
			LogMessages: txWithMeta.Meta.LogMessages,
		})
	}

	return block, nil
}
