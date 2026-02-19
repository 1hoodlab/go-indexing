package handlers

import (
	"context"
	"sync"

	"github.com/1hoodlab/go-indexing.git/internal/models"
	"github.com/1hoodlab/go-indexing.git/pkg/utils"
)

// Registry holds registered event handlers. Key is topic0 (event signature hash).
type Registry struct {
	mu        sync.RWMutex
	handlers  map[string]Handler
	defaultH  Handler
}

// NewRegistry creates a new handler registry with default save-to-logs handler.
func NewRegistry(defaultHandler Handler) *Registry {
	if defaultHandler == nil {
		defaultHandler = func(ctx context.Context, batch []*models.Log) error { return nil }
	}
	return &Registry{
		handlers: make(map[string]Handler),
		defaultH: defaultHandler,
	}
}

// Register registers a handler for the given canonical event signature.
// Canonical format: EventName(type1,type2,...) e.g. Transfer(address,address,uint256)
func (r *Registry) Register(canonical string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	topic0 := utils.EventTopic0(canonical)
	r.handlers[topic0] = h
}

// RegisterByTopic0 registers a handler by raw topic0 hash (0x-prefixed hex).
func (r *Registry) RegisterByTopic0(topic0 string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic0] = h
}

// Handle dispatches a batch to the registered handler, or default if none.
func (r *Registry) Handle(ctx context.Context, topic0 string, batch []*models.Log) error {
	if len(batch) == 0 {
		return nil
	}

	r.mu.RLock()
	h, ok := r.handlers[topic0]
	r.mu.RUnlock()

	if !ok {
		h = r.defaultH
	}

	return h(ctx, batch)
}
