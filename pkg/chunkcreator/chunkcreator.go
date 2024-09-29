package chunkcreator

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/pkg/model"
)

type ChunkCreator struct {
	// chunkSize is a size in bytes of a chunk.
	chunkSize           int64
	chunkWriterFn       func() (model.Writer, error)
	chunkReaderFn       func(model.Writer) (model.Reader, error)
	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(chunkSize int64, chunkReaderFn func(model.Writer) (model.Reader, error), chunkWriterFn func() (model.Writer, error)) *ChunkCreator {
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
	currChunkSize := int64(0)
	currChunk, err := cc.chunkWriterFn()
	if err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	chunk, err := cc.chunkReaderFn(currChunk)
	if err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case chunks <- chunk:
	}

	for {
		foundNew := input.Next()

		if (!foundNew || currChunkSize >= cc.chunkSize) && currChunkSize > 0 {
			currChunkSize = 0

			cc.trace("closing chunk")
			err := currChunk.Close()
			if err != nil {
				return fmt.Errorf("failed to close chunk: %w", err)
			}

			if !foundNew {
				cc.trace("closing chunkWriters")

				break
			}

			currChunk, err = cc.chunkWriterFn()
			if err != nil {
				return fmt.Errorf("failed to create chunk: %w", err)
			}

			currChunkRdr, err := cc.chunkReaderFn(currChunk)
			if err != nil {
				return fmt.Errorf("failed to create chunk: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunks <- currChunkRdr:
				cc.trace("added chunk to chunkWriters")
			}

		}

		row, n, err := input.Read()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		cc.tracef("read row: %v", row)

		err = currChunk.WriteRow(ctx, row)
		if err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}

		cc.tracef("wrote row: %v", row)

		currChunkSize += n
	}

	if input.Err() != nil {
		return fmt.Errorf("failed to read row: %w", input.Err())
	}

	return nil
}

func (cc *ChunkCreator) SyncCreate(ctx context.Context, input model.Reader, chunks chan<- model.Reader) error {
	currChunkSize := int64(0)
	currChunk, err := cc.chunkWriterFn()
	if err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	for {
		foundNew := input.Next()

		if (!foundNew || currChunkSize >= cc.chunkSize) && currChunkSize > 0 {

			currChunkSize = 0
			cc.trace("closing chunk")
			err := currChunk.Close()
			if err != nil {
				return fmt.Errorf("failed to close chunk: %w", err)
			}

			currrChunkRdr, err := cc.chunkReaderFn(currChunk)
			if err != nil {
				return fmt.Errorf("failed to create chunk: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunks <- currrChunkRdr:
				cc.trace("added chunk to chunkWriters")
				if !foundNew {
					break
				}

				cc.trace("creating new chunk")
				currChunk, err = cc.chunkWriterFn()
				if err != nil {
					return fmt.Errorf("failed to create chunk: %w", err)
				}
			}
		}

		if !foundNew {
			cc.trace("closing chunkWriters")
			// close(chunkWriters)
			break
		}

		row, n, err := input.Read()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		cc.tracef("read row: %v", row)

		err = currChunk.WriteRow(ctx, row)
		if err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}

		cc.tracef("wrote row: %v", row)

		currChunkSize += n
	}

	if input.Err() != nil {
		return fmt.Errorf("failed to read row: %w", input.Err())
	}

	return nil
}

var _ model.ChunkCreator = (*ChunkCreator)(nil)
