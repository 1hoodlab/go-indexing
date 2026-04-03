package core

// BlockEvent is a chain-agnostic block notification.
// For EVM this is a block number, for Solana a slot, for Sui a checkpoint.
type BlockEvent struct {
	Number    uint64
	Hash      string
	Timestamp uint64
}

// RawEvent is a chain-specific event before parsing.
// LogFetcher produces these; handlers and storage consume them.
type RawEvent struct {
	BlockNumber     uint64
	TransactionHash string
	Address         string            // contract/program address
	EventKey        string            // identifier for registry dispatch
	RawData         []byte            // chain-specific raw bytes
	Metadata        map[string]string // chain-specific extra fields
}

// ContractConfig is a chain-agnostic contract/program reference for the pipeline.
// Each chain's StorageInterface.LoadContracts() maps its own Contract model to this.
type ContractConfig struct {
	Address     string   // contract address / program ID / package_id
	Events      []string // event identifiers to filter
	StartBlock  uint64   // start block / slot / checkpoint
	LastScanned uint64   // last scanned block / slot / checkpoint
}
