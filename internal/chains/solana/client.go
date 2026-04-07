package solana

import (
	"context"
	"errors"
	"log"

	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// Block is a simplified Solana block for indexing.
type Block struct {
	Slot         uint64
	Blockhash    string
	BlockTime    int64 // Unix timestamp
	Transactions []Transaction
}

// Transaction holds the indexing-relevant fields from a Solana transaction.
type Transaction struct {
	Signature   string   // base58 first signature
	AccountKeys []string // all account keys (message + loaded addresses)
	LogMessages []string // meta.logMessages
}

// Client wraps a gagliardetto RPC client with confirmed commitment baked in.
type Client struct {
	rpc *solanarpc.Client
}

// NewClient creates a Client connected to the given RPC URL.
func NewClient(url string) *Client {
	return &Client{rpc: solanarpc.New(url)}
}

// GetSlot returns the current confirmed slot.
func (c *Client) GetSlot(ctx context.Context) (uint64, error) {
	return c.rpc.GetSlot(ctx, solanarpc.CommitmentConfirmed)
}

// GetBlocksWithLimit returns up to limit confirmed block slot numbers starting at startSlot.
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

// GetBlock fetches a full block with transaction details.
// Returns nil, nil if the slot has no block (not confirmed).
func (c *Client) GetBlock(ctx context.Context, slot uint64) (*Block, error) {
	maxVersion := solanarpc.MaxSupportedTransactionVersion0
	excludeRewards := true

	result, err := c.rpc.GetBlockWithOpts(ctx, slot, &solanarpc.GetBlockOpts{
		Commitment:                     solanarpc.CommitmentConfirmed,
		Encoding:                       solanago.EncodingBase64,
		TransactionDetails:             solanarpc.TransactionDetailsFull,
		MaxSupportedTransactionVersion: &maxVersion,
		Rewards:                        &excludeRewards,
	})
	if err != nil {
		// ErrNotConfirmed means the slot has no block — treat as nil.
		if errors.Is(err, solanarpc.ErrNotConfirmed) {
			return nil, nil
		}
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
			log.Printf("solana client: slot %d: failed to decode transaction: %v", slot, err)
			continue
		}
		if len(tx.Signatures) == 0 {
			continue
		}

		// Merge message account keys + loaded addresses (versioned tx support).
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
