package evm

import (
	"context"
	"errors"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
	"github.com/ethereum/go-ethereum/core/types"
)

// IndexerStore is the subset of postgres.Store the block indexer depends on.
type IndexerStore interface {
	LoadIndexerRuntimeConfig(ctx context.Context) (*postgres.IndexerRuntimeConfig, error)
	ConfirmPendingBlocks(ctx context.Context, networkID int64, confirmedHead uint64) (int, error)
	PersistBlock(ctx context.Context, networkID, chainID int64, block postgres.BlockRow, logs []postgres.LogInsert, enqueueOutbox bool, nextCheckpoint uint64) (*postgres.PersistBlockResult, error)
	PersistCatchupBatch(ctx context.Context, networkID, chainID int64, logs []postgres.LogInsert, enqueueOutbox bool, nextCheckpoint uint64) ([]int64, error)
	GetBlockByNumber(ctx context.Context, networkID int64, blockNumber uint64) (*postgres.BlockRow, error)
	DeleteBlockByHash(ctx context.Context, networkID int64, blockHash string) error
	SetCheckpoint(ctx context.Context, networkID int64, block uint64) error
}

type BlockIndexer struct {
	cfg          config.Config
	store        IndexerStore
	runtime      *postgres.IndexerRuntimeConfig
	pool         *RPCPool
	blockFetcher *BlockFetcher
	rangeFetcher *RangeLogFetcher
	processor    *LogProcessor
}

func NewBlockIndexer(cfg config.Config, store IndexerStore, runtime *postgres.IndexerRuntimeConfig, pool *RPCPool) *BlockIndexer {
	contractIDByAddr := make(map[string]int64)
	abiByAddr := make(map[string][]byte)
	for _, ev := range runtime.ContractEvents {
		contractIDByAddr[ev.Address] = ev.ContractID
		if len(ev.ABI) > 0 {
			abiByAddr[ev.Address] = ev.ABI
		}
	}
	processor := NewLogProcessor(runtime.AllowedTopic0ByAddr, abiByAddr, runtime.EventNameByAddrTopic0, contractIDByAddr)
	return &BlockIndexer{
		cfg:          cfg,
		store:        store,
		runtime:      runtime,
		pool:         pool,
		processor:    processor,
		blockFetcher: NewBlockFetcher(pool, processor, cfg.RetryAfterReceiptsNull),
		rangeFetcher: NewRangeLogFetcher(pool, processor),
	}
}

func (b *BlockIndexer) Run(ctx context.Context) error {
	log.Printf("postgres indexer started (checkpoint=%d, contracts=%d)", b.runtime.Checkpoint, len(b.runtime.Addresses))
	scanning := b.runtime.ScanEnabled
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runtime, err := b.store.LoadIndexerRuntimeConfig(ctx)
		if err != nil {
			return err
		}
		b.runtime = runtime
		if len(runtime.Addresses) == 0 {
			return errors.New("no active contracts with event signatures")
		}

		head, err := b.pool.HeaderByNumber(ctx, nil)
		if err != nil {
			log.Printf("header error: %v", err)
			time.Sleep(b.cfg.RetryAfterFailed)
			continue
		}
		headNum := head.Number.Uint64()
		confirmedHead := headNum
		if headNum > uint64(runtime.ConfirmationDepth) {
			confirmedHead = headNum - uint64(runtime.ConfirmationDepth)
		}

		// Promote pending rows whenever chain depth is sufficient (even when scan is paused).
		if n, err := b.store.ConfirmPendingBlocks(ctx, runtime.NetworkID, confirmedHead); err != nil {
			log.Printf("confirm pending error: %v", err)
		} else if n > 0 {
			log.Printf("confirmed %d pending block(s) up to %d (head=%d, depth=%d)", n, confirmedHead, headNum, runtime.ConfirmationDepth)
		}

		if !runtime.ScanEnabled {
			if scanning {
				log.Printf("scan paused (scan_enabled=false in indexer_config)")
				scanning = false
			}
			time.Sleep(b.cfg.PollIntervalWhenConfirmed)
			continue
		}
		if !scanning {
			log.Printf("scan resumed (scan_enabled=true)")
			scanning = true
		}

		checkpoint := runtime.Checkpoint

		if checkpoint == 0 {
			if err := b.processGenesis(ctx, head, confirmedHead); err != nil {
				log.Printf("genesis error: %v", err)
				time.Sleep(b.cfg.RetryAfterFailed)
			}
			continue
		}

		if confirmedHead >= checkpoint {
			if err := b.processCatchup(ctx, checkpoint, confirmedHead); err != nil {
				log.Printf("catchup error: %v", err)
				time.Sleep(b.cfg.RetryAfterFailed)
			}
			continue
		}

		if b.cfg.TipFollowingEnabled {
			if err := b.processTip(ctx, checkpoint, head); err != nil {
				log.Printf("tip error: %v", err)
				time.Sleep(b.cfg.RetryAfterFailed)
			}
			continue
		}

		time.Sleep(b.cfg.PollIntervalWhenConfirmed)
	}
}

func (b *BlockIndexer) processGenesis(ctx context.Context, head *types.Header, confirmedHead uint64) error {
	ts := time.Unix(int64(head.Time), 0).UTC()
	logs, err := b.blockFetcher.FetchBlockLogs(ctx, head.Hash(), ts)
	if err != nil {
		return err
	}
	enqueue := head.Number.Uint64() <= confirmedHead
	return b.persistSingleBlockHeader(ctx, head, logs, head.Number.Uint64(), enqueue)
}

// processCatchup scans [checkpoint, confirmedHead]; the checkpoint is the next block to scan.
func (b *BlockIndexer) processCatchup(ctx context.Context, checkpoint, confirmedHead uint64) error {
	from := checkpoint
	batchSize := b.cfg.BlockBatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	for from <= confirmedHead {
		runtime, err := b.store.LoadIndexerRuntimeConfig(ctx)
		if err != nil {
			return err
		}
		if !runtime.ScanEnabled {
			log.Printf("catchup interrupted at block %d (scan_enabled=false)", from)
			return nil
		}
		b.runtime = runtime

		to := confirmedHead
		if to-from+1 > batchSize {
			to = from + batchSize - 1
		}

		logs, err := b.rangeFetcher.FetchLogsRange(ctx, from, to, b.runtime.Addresses)
		if err != nil {
			span := to - from + 1
			if span > 1 {
				nextBatch := span / 2
				if nextBatch < 1 {
					nextBatch = 1
				}
				log.Printf("catchup getLogs %d-%d failed (%v), retrying with %d blocks", from, to, err, nextBatch)
				batchSize = nextBatch
				continue
			}
			return err
		}

		inserts := toLogInserts(logs)
		if _, err := b.store.PersistCatchupBatch(ctx, b.runtime.NetworkID, b.runtime.ChainID, inserts, true, to+1); err != nil {
			return err
		}
		log.Printf("catchup blocks %d-%d: %d logs (confirmed_head=%d, depth=%d)",
			from, to, len(logs), confirmedHead, b.runtime.ConfirmationDepth)
		from = to + 1
		time.Sleep(b.cfg.SleepAfterCatchupBatch)
	}
	return nil
}

// processTip indexes the block at the checkpoint (the next block to scan).
func (b *BlockIndexer) processTip(ctx context.Context, checkpoint uint64, head *types.Header) error {
	next := checkpoint
	if head.Number.Uint64() < next {
		time.Sleep(b.cfg.NewBlockWaitingTime)
		return nil
	}

	blockHeader, err := b.pool.HeaderByNumber(ctx, new(big.Int).SetUint64(next))
	if err != nil || blockHeader == nil {
		time.Sleep(b.cfg.NewBlockWaitingTime)
		return err
	}

	existing, err := b.store.GetBlockByNumber(ctx, b.runtime.NetworkID, next-1)
	if err == nil && existing != nil {
		if existing.BlockHash != blockHeader.ParentHash.Hex() {
			log.Printf("reorg at %d: expected parent %s got %s", next, existing.BlockHash, blockHeader.ParentHash.Hex())
			if err := b.store.DeleteBlockByHash(ctx, b.runtime.NetworkID, existing.BlockHash); err != nil {
				return err
			}
			if err := b.store.SetCheckpoint(ctx, b.runtime.NetworkID, next-1); err != nil {
				return err
			}
			return nil
		}
	}

	ts := time.Unix(int64(blockHeader.Time), 0).UTC()
	logs, err := b.blockFetcher.FetchBlockLogs(ctx, blockHeader.Hash(), ts)
	if err != nil {
		return err
	}

	confirmedHead := head.Number.Uint64()
	if head.Number.Uint64() > uint64(b.runtime.ConfirmationDepth) {
		confirmedHead = head.Number.Uint64() - uint64(b.runtime.ConfirmationDepth)
	}
	enqueue := next <= confirmedHead
	return b.persistSingleBlockHeader(ctx, blockHeader, logs, next, enqueue)
}

func (b *BlockIndexer) persistSingleBlockHeader(ctx context.Context, header *types.Header, logs []ProcessedLog, nextCheckpoint uint64, enqueue bool) error {
	ts := time.Unix(int64(header.Time), 0).UTC()
	status := "pending"
	if enqueue {
		status = "confirmed"
	}
	blockRow := postgres.BlockRow{
		NetworkID:   b.runtime.NetworkID,
		ChainID:     b.runtime.ChainID,
		BlockNumber: header.Number.Uint64(),
		BlockHash:   header.Hash().Hex(),
		ParentHash:  header.ParentHash.Hex(),
		Timestamp:   ts,
		Status:      status,
		IsCanonical: true,
	}
	inserts := toLogInserts(logs)
	parentHash := strings.ToLower(header.ParentHash.Hex())
	for i := range inserts {
		inserts[i].ParentHash = parentHash
	}
	_, err := b.store.PersistBlock(ctx, b.runtime.NetworkID, b.runtime.ChainID, blockRow, inserts, enqueue, nextCheckpoint+1)
	if err != nil {
		return err
	}
	log.Printf("block %d: %d logs (status=%s)", header.Number.Uint64(), len(logs), status)
	return nil
}

func toLogInserts(logs []ProcessedLog) []postgres.LogInsert {
	inserts := make([]postgres.LogInsert, len(logs))
	for i, l := range logs {
		inserts[i] = postgres.LogInsert{
			ContractID:       l.ContractID,
			ContractAddress:  l.ContractAddress,
			BlockNumber:      l.BlockNumber,
			BlockHash:        l.BlockHash,
			ParentHash:       l.ParentHash,
			BlockTimestamp:   l.BlockTimestamp,
			TransactionHash:  l.TransactionHash,
			TransactionIndex: l.TransactionIndex,
			LogIndex:         l.LogIndex,
			EventName:        l.EventName,
			Topics:           l.Topics,
			Data:             l.Data,
			DecodedParams:    l.DecodedParams,
		}
	}
	return inserts
}
