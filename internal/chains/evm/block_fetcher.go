package evm

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type BlockFetcher struct {
	client       RPCClient
	processor    *LogProcessor
	retryDelay   time.Duration
	maxRetries   int
}

func NewBlockFetcher(client RPCClient, processor *LogProcessor, retryDelay time.Duration) *BlockFetcher {
	return &BlockFetcher{
		client:     client,
		processor:  processor,
		retryDelay: retryDelay,
		maxRetries: 5,
	}
}

func (f *BlockFetcher) FetchBlockLogs(ctx context.Context, blockHash common.Hash, blockTimestamp time.Time) ([]ProcessedLog, error) {
	receipts, err := GetBlockReceiptsWithRetry(ctx, f.client, blockHash, f.maxRetries, f.retryDelay)
	if err != nil {
		return nil, err
	}
	logs := FlattenReceiptLogs(receipts)
	return f.processor.ProcessLogs(logs, blockTimestamp), nil
}
