package core

import "context"

type Subscriber interface {
	SubscribeBlocks(ctx context.Context, from uint64, out chan<- BlockEvent) error
}

type LogFetcher interface {
	FetchLogs(ctx context.Context, block BlockEvent, contracts []ContractConfig) ([]RawEvent, error)
}

type EventParser interface {
	ParseEvent(raw RawEvent) (map[string]interface{}, error)
	EventKey(eventIdentifier string) string
}

type StorageInterface interface {
	SaveEvents(ctx context.Context, events []RawEvent) error
	UpdateScanProgress(ctx context.Context, address string, block uint64) error
	LoadContracts(ctx context.Context) ([]ContractConfig, error)
	EnsureIndexes(ctx context.Context) error
	Close(ctx context.Context) error
}
