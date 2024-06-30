package chunkcreator

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/internal/model"
)

type ChunkCreator struct {
	// chunkSize is a size of a chunk.
	chunkSize           int
	chunkWriterFn       func() model.Writer
	chunkReaderFn       func(model.Writer) model.Reader
	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(chunkSize int, chunkReaderFn func(model.Writer) model.Reader, chunkWriterFn func() model.Writer) *ChunkCreator {
	return &ChunkCreator{
		chunkSize:     chunkSize,
		chunkWriterFn: chunkWriterFn,
		chunkReaderFn: chunkReaderFn,
	}
}

func (cc *ChunkCreator) SetLogger(logger model.Logger) {
	cc.defaultLoggerFields = map[string]interface{}{
		"component": "chunkCreator",
	}

	cc.logger = logger
}

func (cc *ChunkCreator) Create(ctx context.Context, input model.Reader, chunks chan<- model.Reader) error {
	currCount := 0
	currChunk := cc.chunkWriterFn()
	for {
		foundNew := input.Next()

		if (!foundNew || currCount >= cc.chunkSize) && currCount > 0 {

			currCount = 0
			cc.trace("closing chunk")
			err := currChunk.Close()
			if err != nil {
				return fmt.Errorf("failed to close chunk: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunks <- cc.chunkReaderFn(currChunk):
				cc.trace("added chunk to chunkWriters")
				if !foundNew {
					break
				}

				cc.trace("creating new chunk")
				currChunk = cc.chunkWriterFn()
			}
		}

		if !foundNew {
			cc.trace("closing chunkWriters")
			// close(chunkWriters)
			break
		}

		row, err := input.Read()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		cc.tracef("read row: %v", row)

		err = currChunk.WriteRow(ctx, row)
		if err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}

		cc.tracef("wrote row: %v", row)

		currCount++
	}

	if input.Err() != nil {
		return fmt.Errorf("failed to read row: %w", input.Err())
	}

	return nil
}

var _ model.ChunkCreator = (*ChunkCreator)(nil)
