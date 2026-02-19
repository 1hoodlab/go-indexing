package labels

import "context"

// Label represents an address or contract label.
type Label struct {
	Address string
	Name    string
	Type    string // e.g. "contract", "eoaa", "token"
}

// Resolver resolves labels for addresses.
type Resolver interface {
	// Resolve returns labels for the given addresses.
	Resolve(ctx context.Context, addresses []string) (map[string]*Label, error)
}

// NoopResolver implements Resolver with no enrichment.
type NoopResolver struct{}

// Resolve returns empty labels for all addresses.
func (n *NoopResolver) Resolve(ctx context.Context, addresses []string) (map[string]*Label, error) {
	return make(map[string]*Label), nil
}
