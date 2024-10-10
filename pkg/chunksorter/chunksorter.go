package chunksorter

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/model"
)

// ChunkSorter is a sorter for chunks.
type ChunkSorter struct {
	keyFn         model.AllocateKeyFn
	vectorFn      vector.AllocateVectorFnfunc
	chunkWriterFn func() (int, model.Writer, error)
	chunkReaderFn func(int) (model.Reader, error)

	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(
	chunkWriterFn func() (int, model.Writer, error),
	chunkReaderFn func(int) (model.Reader, error),
	keyFn model.AllocateKeyFn,
	vectroFn vector.AllocateVectorFnfunc,
) *ChunkSorter {
	return &ChunkSorter{
		chunkWriterFn: chunkWriterFn,
		chunkReaderFn: chunkReaderFn,
		vectorFn:      vectroFn,
		keyFn:         keyFn,
	}
}

func (c *ChunkSorter) SetLogger(logger model.Logger) {
	c.defaultLoggerFields = map[string]interface{}{
		"component": "chunkSorter",
	}

	c.logger = logger
}

func (c *ChunkSorter) validate() error {
	if c.chunkWriterFn == nil {
		return ErrNilChunkWriter
	}

	if c.chunkReaderFn == nil {
		return ErrNilChunkReader
	}

	if c.vectorFn == nil {
		return ErrNilVectorFn
	}

	if c.keyFn == nil {
		return ErrNilKeyFn
	}

	return nil
}

// Sort sorts the chunks.
func (c *ChunkSorter) Sort(ctx context.Context, rdr model.Reader) (model.Reader, error) {

	err := c.validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate ChunkSorter: %w", err)
	}

	c.trace("creating output buffer")

	buffer := c.vectorFn(c.keyFn)
	if buffer == nil {
		return nil, ErrNilVector
	}

	c.trace("output buffer created")

outer:
	for {

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			foundNew := rdr.Next()
			if !foundNew {
				break outer
			}

			row, n, err := rdr.Read()
			if err != nil {
				return nil, err
			}

			c.tracef("pushing row %v to buffer", row)
			err = buffer.PushBack(row, n)
			if err != nil {
				return nil, err
			}
		}
	}

	if rdr.Err() != nil {
		return nil, fmt.Errorf("failed to read row: %w", rdr.Err())
	}

	c.trace("sorting buffer")
	buffer.Sort()

	c.trace("creating chunk writer")
	chunkIdx, wr, err := c.chunkWriterFn()
	if err != nil {
		return nil, fmt.Errorf("failed to create chunk writer: %w", err)
	}

	c.debugf("chunk writer created idx: %d", chunkIdx)

	c.trace("writing rows")
	for i := range buffer.Len() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			c.tracef("writing row %d", i)
			err := wr.WriteRow(ctx, buffer.Get(i).Row)
			if err != nil {
				return nil, fmt.Errorf("failed to write row: %w", err)
			}
		}
	}

	c.trace("resetting output buffer")
	buffer.Reset()

	buffer = nil

	c.trace("closing chunk writer after sorting")
	err = wr.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close chunk writer: %w", err)
	}

	c.infof("chunk %d sorted", chunkIdx)

	c.tracef("creating chunk reader: %d", chunkIdx)

	chunkRdr, err := c.chunkReaderFn(chunkIdx)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunk reader: %w", err)
	}

	c.debugf("chunk reader created idx: %d", chunkIdx)

	return chunkRdr, nil
}
