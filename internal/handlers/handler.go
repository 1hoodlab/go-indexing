package handlers

import (
	"context"

	"github.com/1hoodlab/go-indexing.git/internal/models"
)

// Handler processes a batch of logs of the same event type.
type Handler func(ctx context.Context, batch []*models.Log) error
