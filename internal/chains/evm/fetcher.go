package evm

import (
	"context"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// RangeLogFetcher fetches logs across a block range for catch-up mode.
type RangeLogFetcher struct {
	client    RPCClient
	processor *LogProcessor
}

func NewRangeLogFetcher(client RPCClient, processor *LogProcessor) *RangeLogFetcher {
	return &RangeLogFetcher{client: client, processor: processor}
}

func (f *RangeLogFetcher) FetchLogsRange(ctx context.Context, fromBlock, toBlock uint64, addresses []string) ([]ProcessedLog, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	addrs := make([]common.Address, 0, len(addresses))
	for _, a := range addresses {
		addrs = append(addrs, common.HexToAddress(a))
	}
	from := new(big.Int).SetUint64(fromBlock)
	to := new(big.Int).SetUint64(toBlock)
	logs, err := f.client.FilterLogs(ctx, from, to, addrs)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return nil, nil
	}

	blockMeta := make(map[uint64]struct {
		timestamp  time.Time
		parentHash string
	})
	var out []ProcessedLog
	for _, vLog := range logs {
		meta, ok := blockMeta[vLog.BlockNumber]
		if !ok {
			header, err := f.client.HeaderByNumber(ctx, new(big.Int).SetUint64(vLog.BlockNumber))
			if err != nil {
				return nil, err
			}
			meta = struct {
				timestamp  time.Time
				parentHash string
			}{
				timestamp:  time.Unix(int64(header.Time), 0).UTC(),
				parentHash: strings.ToLower(header.ParentHash.Hex()),
			}
			blockMeta[vLog.BlockNumber] = meta
		}
		processed := f.processor.ProcessLogs([]types.Log{vLog}, meta.timestamp)
		for i := range processed {
			processed[i].ParentHash = meta.parentHash
		}
		out = append(out, processed...)
	}
	return out, nil
}
