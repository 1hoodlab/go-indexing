package evm

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/1hoodlab/go-indexing.git/internal/core"
	"github.com/ethereum/go-ethereum/common"
)

type LogFetcher struct {
	client      RPCClient
	networkID   string
	contractMap map[string]contractInfo
}

type contractInfo struct {
	id  string
	abi string
}

func NewLogFetcher(client RPCClient, networkID string, contracts []*Contract) *LogFetcher {
	cm := make(map[string]contractInfo, len(contracts))
	for _, c := range contracts {
		cm[strings.ToLower(c.Address)] = contractInfo{id: c.ID.Hex(), abi: c.ABI}
	}
	return &LogFetcher{client: client, networkID: networkID, contractMap: cm}
}

func (f *LogFetcher) FetchLogs(ctx context.Context, block core.BlockEvent, contracts []core.ContractConfig) ([]core.RawEvent, error) {
	var addresses []common.Address
	for _, c := range contracts {
		nextBlock := c.StartBlock
		if c.LastScanned > 0 {
			nextBlock = c.LastScanned + 1
		}
		if block.Number >= nextBlock {
			addresses = append(addresses, common.HexToAddress(c.Address))
		}
	}
	if len(addresses) == 0 {
		return nil, nil
	}

	blockNum := new(big.Int).SetUint64(block.Number)
	logs, err := f.client.FilterLogs(ctx, blockNum, blockNum, addresses)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return nil, nil
	}

	header, err := f.client.HeaderByNumber(ctx, blockNum)
	if err != nil {
		return nil, err
	}
	blockHash := header.Hash().Hex()

	events := make([]core.RawEvent, 0, len(logs))
	for _, vLog := range logs {
		contractAddr := strings.ToLower(vLog.Address.Hex())
		ci, ok := f.contractMap[contractAddr]
		if !ok {
			continue
		}

		eventKey := ""
		if len(vLog.Topics) > 0 {
			eventKey = vLog.Topics[0].Hex()
		}

		indexedTopics := make([]string, 0, len(vLog.Topics)-1)
		for i := 1; i < len(vLog.Topics); i++ {
			indexedTopics = append(indexedTopics, vLog.Topics[i].Hex())
		}

		events = append(events, core.RawEvent{
			BlockNumber:     vLog.BlockNumber,
			TransactionHash: vLog.TxHash.Hex(),
			Address:         contractAddr,
			EventKey:        eventKey,
			RawData:         vLog.Data,
			Metadata: map[string]string{
				"network_id":  f.networkID,
				"contract_id": ci.id,
				"block_hash":  blockHash,
				"log_index":   strconv.FormatUint(uint64(vLog.Index), 10),
				"topics":      strings.Join(indexedTopics, ","),
				"data":        fmt.Sprintf("0x%x", vLog.Data),
				"abi":         ci.abi,
			},
		})
	}
	return events, nil
}
