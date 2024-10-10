package chunkcreator

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/pkg/model"
)

type ChunkCreator struct {
	// chunkSize is a size in bytes of a chunk.
	totalChunkMemory    int64
	chunkWriterFn       func() (int, model.Writer, error)
	chunkReaderFn       func(idx int) (model.Reader, error)
	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(
	totalChunkMemory int64,
	chunkReaderFn func(idx int) (model.Reader, error),
	chunkWriterFn func() (int, model.Writer, error),
) *ChunkCreator {
	return &ChunkCreator{
		totalChunkMemory: totalChunkMemory,
		chunkWriterFn:    chunkWriterFn,
		chunkReaderFn:    chunkReaderFn,
	}
}

func (cc *ChunkCreator) SetLogger(logger model.Logger) {
	cc.defaultLoggerFields = map[string]interface{}{
		"component": "chunkCreator",
	}

	cc.logger = logger
}

func (cc *ChunkCreator) MaxMemory() int64 {
	return cc.totalChunkMemory
}

func (cc *ChunkCreator) Create(ctx context.Context, input model.Reader, chunks chan<- model.Reader, chunkMemory int64) error {
	currChunkSize := int64(0)

	cc.tracef("creating chunk writer")

	chunkIDx, currChunk, err := cc.chunkWriterFn()
	if err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	cc.tracef("chunk writer created idx: %d", chunkIDx)

	cc.tracef("creating chunk reader: %d", chunkIDx)
	chunk, err := cc.chunkReaderFn(chunkIDx)
	if err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	cc.debugf("chunk reader created idx: %d", chunkIDx)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chunks <- chunk:
	}

	for {
		foundNew := input.Next()

		if (!foundNew || currChunkSize >= chunkMemory) && currChunkSize > 0 {
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

			chunkIDx, currChunk, err = cc.chunkWriterFn()
			if err != nil {
				return fmt.Errorf("failed to create chunk: %w", err)
			}

			currChunkRdr, err := cc.chunkReaderFn(chunkIDx)
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

func (cc *ChunkCreator) SyncCreate(ctx context.Context, input model.Reader, chunks chan<- model.Reader, chunkMemory int64) error {
	currChunkSize := int64(0)

	cc.tracef("creating chunk writer")

	chunkIdx, currChunk, err := cc.chunkWriterFn()
	if err != nil {
		return fmt.Errorf("failed to create sync chunk: %w", err)
	}

	cc.debugf("chunk writer created idx: %d", chunkIdx)

	for {
		foundNew := input.Next()
		if (!foundNew || currChunkSize >= chunkMemory) && currChunkSize > 0 {

			currChunkSize = 0
			cc.trace("closing sync chunk")
			err := currChunk.Close()
			if err != nil {
				return fmt.Errorf("failed to close sync chunk: %w", err)
			}

			cc.trace("closed sync chunk")

			cc.tracef("creating chunk reader: %d", chunkIdx)

			currrChunkRdr, err := cc.chunkReaderFn(chunkIdx)
			if err != nil {
				return fmt.Errorf("failed to create sync chunk: %w", err)
			}

			cc.infof("chunk %d created", chunkIdx)

			cc.debugf("chunk reader created idx: %d", chunkIdx)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunks <- currrChunkRdr:
				cc.trace("added sync chunk to chunkReaders")
				if !foundNew {
					break
				}

				chunkIdx, currChunk, err = cc.chunkWriterFn()
				if err != nil {
					return fmt.Errorf("failed to create chunk: %w", err)
				}

				cc.infof("chunk writer created idx: %d", chunkIdx)
			}
		}

		if !foundNew {
			cc.trace("closing chunkWriters")
			break
		}

		row, n, err := input.Read()
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}
		cc.tracef("sync read row: %v", row)

		err = currChunk.WriteRow(ctx, row)
		if err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}

		cc.tracef("sync wrote row: %v", row)

		currChunkSize += n
	}

	if input.Err() != nil {
		return fmt.Errorf("failed to read row: %w", input.Err())
	}

	return nil
}

var _ model.ChunkCreator = (*ChunkCreator)(nil)
