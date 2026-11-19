package solana

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	"github.com/1hoodlab/go-indexing.git/internal/storage/postgres"
)

const slotBatchSize = uint64(100)

// SlotIndexerStore is the subset of postgres.Store the slot indexer depends on.
type SlotIndexerStore interface {
	LoadSolanaRuntimeConfig(ctx context.Context) (*postgres.SolanaRuntimeConfig, error)
	PersistSolanaBlock(ctx context.Context, networkID, chainID int64, slot uint64, blockHash string, blockTimestamp time.Time, logs []postgres.SolLogInsert, enqueueOutbox bool, nextCheckpoint uint64) ([]int64, error)
	PersistSolanaCatchupBatch(ctx context.Context, networkID, chainID int64, logs []postgres.SolLogInsert, enqueueOutbox bool, nextCheckpoint uint64) ([]int64, error)
	SetCheckpoint(ctx context.Context, networkID int64, block uint64) error
}

type SlotIndexer struct {
	cfg     config.Config
	store   SlotIndexerStore
	runtime *postgres.SolanaRuntimeConfig
	pool    *RPCPool
	parser  *EventParser
}

func NewSlotIndexer(cfg config.Config, store SlotIndexerStore, runtime *postgres.SolanaRuntimeConfig, pool *RPCPool) *SlotIndexer {
	return &SlotIndexer{
		cfg:     cfg,
		store:   store,
		runtime: runtime,
		pool:    pool,
		parser:  NewEventParser(),
	}
}

func (s *SlotIndexer) Run(ctx context.Context) error {
	log.Printf("solana indexer started (checkpoint=%d, programs=%d)", s.runtime.Checkpoint, len(s.runtime.ProgramAddresses))
	scanning := s.runtime.ScanEnabled

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runtime, err := s.store.LoadSolanaRuntimeConfig(ctx)
		if err != nil {
			return err
		}
		s.runtime = runtime
		if len(runtime.ProgramAddresses) == 0 {
			return errors.New("no active programs with solana event configs")
		}

		currentSlot, err := s.pool.GetSlot(ctx)
		if err != nil {
			log.Printf("solana: getSlot error: %v", err)
			time.Sleep(s.cfg.RetryAfterFailed)
			continue
		}

		if !runtime.ScanEnabled {
			if scanning {
				log.Printf("solana: scan paused (scan_enabled=false)")
				scanning = false
			}
			time.Sleep(s.cfg.PollIntervalWhenConfirmed)
			continue
		}
		if !scanning {
			log.Printf("solana: scan resumed")
			scanning = true
		}

		checkpoint := runtime.Checkpoint
		if checkpoint == 0 {
			checkpoint = currentSlot
			if err := s.store.SetCheckpoint(ctx, runtime.NetworkID, checkpoint); err != nil {
				return err
			}
			log.Printf("solana: no checkpoint, starting from current slot %d", checkpoint)
			continue
		}

		if currentSlot > checkpoint {
			if err := s.processCatchup(ctx, checkpoint, currentSlot); err != nil {
				log.Printf("solana: catchup error: %v", err)
				time.Sleep(s.cfg.RetryAfterFailed)
			}
			continue
		}

		time.Sleep(s.cfg.NewBlockWaitingTime)
	}
}

func (s *SlotIndexer) processCatchup(ctx context.Context, checkpoint, currentSlot uint64) error {
	startSlot := checkpoint

	for startSlot <= currentSlot {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		runtime, err := s.store.LoadSolanaRuntimeConfig(ctx)
		if err != nil {
			return err
		}
		if !runtime.ScanEnabled {
			log.Printf("solana: catchup interrupted at slot %d (scan_enabled=false)", startSlot)
			return nil
		}
		s.runtime = runtime

		limit := currentSlot - startSlot + 1
		if limit > slotBatchSize {
			limit = slotBatchSize
		}

		confirmedSlots, err := s.pool.GetBlocksWithLimit(ctx, startSlot, limit)
		if err != nil {
			return err
		}

		if len(confirmedSlots) == 0 {
			startSlot += limit
			if err := s.store.SetCheckpoint(ctx, runtime.NetworkID, startSlot); err != nil {
				return err
			}
			continue
		}

		programs := s.buildProgramMap()

		var allLogs []postgres.SolLogInsert
		var maxSlot uint64
		for _, slot := range confirmedSlots {
			block, err := s.pool.GetBlock(ctx, slot)
			if err != nil {
				log.Printf("solana: getBlock(%d) error: %v", slot, err)
				continue
			}
			if block == nil {
				continue
			}

			extracted := ExtractBlockLogs(block, programs)
			for i := range extracted {
				decoded := s.tryDecode(extracted[i])
				allLogs = append(allLogs, toSolLogInsert(extracted[i], s.runtime.NetworkID, decoded))
			}
			if slot > maxSlot {
				maxSlot = slot
			}
		}

		nextCheckpoint := maxSlot + 1
		if _, err := s.store.PersistSolanaCatchupBatch(ctx, runtime.NetworkID, runtime.ChainID, allLogs, true, nextCheckpoint); err != nil {
			return err
		}
		log.Printf("solana: catchup slots %d-%d: %d logs (tip=%d)", startSlot, maxSlot, len(allLogs), currentSlot)

		startSlot = nextCheckpoint
		time.Sleep(s.cfg.SleepAfterCatchupBatch)
	}
	return nil
}

func (s *SlotIndexer) buildProgramMap() map[string]*ProgramInfo {
	programs := make(map[string]*ProgramInfo)
	for _, addr := range s.runtime.ProgramAddresses {
		programs[addr] = &ProgramInfo{
			ContractID:           s.runtime.ContractIDByAddr[addr],
			IDL:                  string(s.runtime.IDLByAddr[addr]),
			DiscriminatorToEvent: s.runtime.DiscriminatorToEventByAddr[addr],
		}
	}
	return programs
}

func (s *SlotIndexer) tryDecode(l ProcessedSolLog) map[string]any {
	addr := l.ProgramAddress
	idlJSON := string(s.runtime.IDLByAddr[addr])
	if idlJSON == "" {
		return nil
	}
	_, decoded, err := s.parser.ParseEvent(l.RawData, idlJSON)
	if err != nil {
		return nil
	}
	return decoded
}

func toSolLogInsert(l ProcessedSolLog, networkID int64, decoded map[string]any) postgres.SolLogInsert {
	return postgres.SolLogInsert{
		ContractID:     l.ContractID,
		ProgramAddress: l.ProgramAddress,
		Slot:           l.Slot,
		BlockHash:      l.BlockHash,
		BlockTimestamp: time.Unix(l.BlockTimestamp, 0).UTC(),
		TxHash:         l.TxHash,
		LogIndex:       l.LogIndex,
		Discriminator:  l.Discriminator,
		EventKey:       l.EventKey,
		EventName:      l.EventName,
		RawData:        l.RawData,
		DecodedParams:  decoded,
		AccountKeys:    l.AccountKeys,
	}
}
