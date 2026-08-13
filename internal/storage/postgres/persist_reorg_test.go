package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Integration tests against a real PostgreSQL.
// Run with: TEST_POSTGRES_DSN=postgres://user:pass@localhost:5432/db?sslmode=disable go test ./internal/storage/postgres/
// Skipped when TEST_POSTGRES_DSN is not set.

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("goose dialect: %v", err)
	}
	if err := goose.Up(db, "../../../migrations"); err != nil {
		t.Fatalf("goose up: %v", err)
	}
	if _, err := db.Exec(`TRUNCATE outbox, eth_logs, blocks, evm_event_signatures, contracts, indexer_config, networks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	store, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func seedNetworkAndContract(t *testing.T, s *Store) (networkID, contractID int64) {
	t.Helper()
	ctx := context.Background()
	err := s.pool.QueryRow(ctx, `
		INSERT INTO networks (name, chain_id, rpc_urls) VALUES ('testnet', 31337, '["http://localhost"]')
		RETURNING id`).Scan(&networkID)
	if err != nil {
		t.Fatalf("seed network: %v", err)
	}
	err = s.pool.QueryRow(ctx, `
		INSERT INTO contracts (network_id, name, address)
		VALUES ($1, 'token', '0x00000000000000000000000000000000000000aa')
		RETURNING id`, networkID).Scan(&contractID)
	if err != nil {
		t.Fatalf("seed contract: %v", err)
	}
	return networkID, contractID
}

const (
	hashA = "0x" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "0x" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	txH   = "0x" + "1111111111111111111111111111111111111111111111111111111111111111"
)

func testLog(contractID int64, blockHash string) LogInsert {
	return LogInsert{
		ContractID:       contractID,
		ContractAddress:  "0x00000000000000000000000000000000000000aa",
		BlockNumber:      100,
		BlockHash:        blockHash,
		BlockTimestamp:   time.Unix(1700000000, 0).UTC(),
		TransactionHash:  txH,
		TransactionIndex: 0,
		LogIndex:         0,
		EventName:        "Transfer",
		Topics:           []string{"0x" + "2222222222222222222222222222222222222222222222222222222222222222"},
		Data:             "0x",
	}
}

func testBlock(networkID int64, blockHash string) BlockRow {
	return BlockRow{
		NetworkID:   networkID,
		ChainID:     31337,
		BlockNumber: 100,
		BlockHash:   blockHash,
		Timestamp:   time.Unix(1700000000, 0).UTC(),
		Status:      "confirmed",
		IsCanonical: true,
	}
}

// After a reorg orphans a block, re-indexing the replacement block at the same
// height with the same transactions must reclaim the orphaned log row (attach
// it to the new canonical block, mark it confirmed) and re-enqueue its outbox
// event so consumers receive the canonical version.
func TestPersistBlock_ReclaimsOrphanedLogAfterReorg(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	networkID, contractID := seedNetworkAndContract(t, store)

	resA, err := store.PersistBlock(ctx, networkID, 31337, testBlock(networkID, hashA), []LogInsert{testLog(contractID, hashA)}, true, 101)
	if err != nil {
		t.Fatalf("persist block A: %v", err)
	}
	if len(resA.LogIDs) != 1 {
		t.Fatalf("block A: expected 1 log inserted, got %d", len(resA.LogIDs))
	}

	if err := store.DeleteBlockByHash(ctx, networkID, hashA); err != nil {
		t.Fatalf("orphan block A: %v", err)
	}

	resB, err := store.PersistBlock(ctx, networkID, 31337, testBlock(networkID, hashB), []LogInsert{testLog(contractID, hashB)}, true, 101)
	if err != nil {
		t.Fatalf("persist block B: %v", err)
	}
	if len(resB.LogIDs) != 1 {
		t.Fatalf("block B: expected orphaned log to be reclaimed (1 log id), got %d", len(resB.LogIDs))
	}

	var status, blockHash string
	var blockID int64
	err = store.pool.QueryRow(ctx, `
		SELECT status::text, block_hash, block_id FROM eth_logs
		WHERE chain_id = 31337 AND block_number = 100 AND transaction_hash = $1 AND log_index = 0`,
		txH,
	).Scan(&status, &blockHash, &blockID)
	if err != nil {
		t.Fatalf("load log: %v", err)
	}
	if status != "confirmed" {
		t.Errorf("log status = %s, want confirmed", status)
	}
	if blockHash != hashB {
		t.Errorf("log block_hash = %s, want %s (new canonical block)", blockHash, hashB)
	}
	if blockID != resB.BlockID {
		t.Errorf("log block_id = %d, want %d (new canonical block)", blockID, resB.BlockID)
	}

	var outboxStatus string
	err = store.pool.QueryRow(ctx, `SELECT status::text FROM outbox WHERE log_id = $1`, resB.LogIDs[0]).Scan(&outboxStatus)
	if err != nil {
		t.Fatalf("load outbox: %v", err)
	}
	if outboxStatus != "pending" {
		t.Errorf("outbox status = %s, want pending (event must be re-delivered for canonical block)", outboxStatus)
	}
}

// Re-persisting the same block (e.g. a retried batch) must not duplicate the
// log or re-enqueue a second outbox delivery.
func TestPersistBlock_DuplicatePersistDoesNotReenqueue(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	networkID, contractID := seedNetworkAndContract(t, store)

	resA, err := store.PersistBlock(ctx, networkID, 31337, testBlock(networkID, hashA), []LogInsert{testLog(contractID, hashA)}, true, 101)
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if len(resA.LogIDs) != 1 {
		t.Fatalf("expected 1 log inserted, got %d", len(resA.LogIDs))
	}

	resDup, err := store.PersistBlock(ctx, networkID, 31337, testBlock(networkID, hashA), []LogInsert{testLog(contractID, hashA)}, true, 101)
	if err != nil {
		t.Fatalf("persist duplicate: %v", err)
	}
	if len(resDup.LogIDs) != 0 {
		t.Fatalf("duplicate persist must not reclaim the live log, got %d log ids", len(resDup.LogIDs))
	}

	var logCount, outboxCount int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs`).Scan(&logCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if logCount != 1 || outboxCount != 1 {
		t.Fatalf("expected 1 log and 1 outbox row, got %d logs, %d outbox", logCount, outboxCount)
	}
}
