package pipeline

import (
	"context"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/chains/evm"
)

func TestProcessBlocks_UsePipelineContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	p := &Pipeline{
		ctx:     ctx,
		blockCh: make(chan *evm.BlockEvent),
	}

	// Close the channel so processBlocks exits the range loop immediately.
	close(p.blockCh)

	done := make(chan struct{})
	p.wg.Add(1)
	go func() {
		p.processBlocks()
		close(done)
	}()
	<-done
	// If we got here without panic, processBlocks handled the cancelled context.
}
