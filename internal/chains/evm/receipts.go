package evm

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type RpcReceipt struct {
	TransactionHash  common.Hash
	TransactionIndex uint
	Logs             []types.Log
}

func FlattenReceiptLogs(receipts []RpcReceipt) []types.Log {
	var logs []types.Log
	for _, r := range receipts {
		logs = append(logs, r.Logs...)
	}
	return logs
}

func GetBlockReceiptsWithRetry(
	ctx context.Context,
	client RPCClient,
	blockHash common.Hash,
	maxRetries int,
	retryDelay time.Duration,
) ([]RpcReceipt, error) {
	var receipts []RpcReceipt
	var err error
	for i := 0; i < maxRetries; i++ {
		receipts, err = client.GetBlockReceipts(ctx, blockHash)
		if err != nil {
			return nil, err
		}
		if receipts != nil {
			return receipts, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return nil, ErrReceiptsNotReady
}
