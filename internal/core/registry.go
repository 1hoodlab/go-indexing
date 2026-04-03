package core

import (
	"context"
	"sync"
)

type Handler func(ctx context.Context, batch []RawEvent) error

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	defaultH Handler
}

func NewRegistry(defaultHandler Handler) *Registry {
	if defaultHandler == nil {
		defaultHandler = func(ctx context.Context, batch []RawEvent) error { return nil }
	}
	return &Registry{
		handlers: make(map[string]Handler),
		defaultH: defaultHandler,
	}
}

func (r *Registry) Register(eventKey string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[eventKey] = h
}

func (r *Registry) Dispatch(ctx context.Context, eventKey string, batch []RawEvent) error {
	if len(batch) == 0 {
		return nil
	}
	r.mu.RLock()
	h, ok := r.handlers[eventKey]
	r.mu.RUnlock()
	if !ok {
		h = r.defaultH
	}
	return h(ctx, batch)
}
