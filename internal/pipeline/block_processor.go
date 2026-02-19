package pipeline

import (
	"context"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/ethereum/go-ethereum/common"
	"go.mongodb.org/mongo-driver/bson"
)

// BlockProcessor processes a single block: fetch logs, group by topic0, dispatch to handlers.
func (p *Pipeline) processBlock(ctx context.Context, blockNum *big.Int) error {
	// Contracts that need this block (blockNum >= NextBlock)
	var toScan []*models.Contract
	for _, c := range p.contracts {
		if blockNum.Uint64() >= c.NextBlock() {
			toScan = append(toScan, c)
		}
	}
	if len(toScan) == 0 {
		return nil
	}

	addresses := make([]common.Address, len(toScan))
	addrToContract := make(map[string]*models.Contract)
	for i, c := range toScan {
		addr := common.HexToAddress(c.Address)
		addresses[i] = addr
		addrToContract[strings.ToLower(c.Address)] = c
	}

	logs, err := p.evm.FilterLogs(ctx, blockNum, blockNum, addresses)
	if err != nil {
		return err
	}

	if len(logs) > 0 {
		log.Printf("[%s] block %s: %d logs from %d contracts", p.network.Name, blockNum.String(), len(logs), len(toScan))
	}

	if len(logs) == 0 {
		return p.updateLastScannedBlock(ctx, blockNum.Uint64(), toScan)
	}

	// Build block header for hash (we may not have full block)
	header, err := p.evm.HeaderByNumber(ctx, blockNum)
	if err != nil {
		return err
	}
	blockHash := header.Hash().Hex()

	// Group logs by topic0 (event_signature)
	batches := make(map[string][]*models.Log)
	for _, vLog := range logs {
		topic0 := ""
		if len(vLog.Topics) > 0 {
			topic0 = vLog.Topics[0].Hex()
		}

		contractAddr := strings.ToLower(vLog.Address.Hex())
		c, ok := addrToContract[contractAddr]
		if !ok {
			continue
		}

		topics := make([]string, 0, len(vLog.Topics)-1)
		for i := 1; i < len(vLog.Topics); i++ {
			topics = append(topics, vLog.Topics[i].Hex())
		}

		doc := &models.Log{
			NetworkID:       p.network.ID,
			ContractID:      c.ID,
			BlockNumber:     vLog.BlockNumber,
			BlockHash:       blockHash,
			TransactionHash: vLog.TxHash.Hex(),
			LogIndex:        uint32(vLog.Index),
			Address:         contractAddr,
			EventSignature:  topic0,
			Topics:          topics,
			Data:            "0x" + common.Bytes2Hex(vLog.Data),
		}

		batches[topic0] = append(batches[topic0], doc)
	}

	// Dispatch each batch to handler
	for topic0, batch := range batches {
		if err := p.registry.Handle(ctx, topic0, batch); err != nil {
			log.Printf("handler error (topic0=%s): %v", topic0, err)
			return err
		}
	}

	return p.updateLastScannedBlock(ctx, blockNum.Uint64(), toScan)
}

func (p *Pipeline) updateLastScannedBlock(ctx context.Context, blockNum uint64, contracts []*models.Contract) error {
	for _, c := range contracts {
		_, err := p.store.Contracts().UpdateOne(ctx,
			bson.M{"_id": c.ID},
			bson.M{"$set": bson.M{"last_scanned_block": blockNum, "updated_at": time.Now()}},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
