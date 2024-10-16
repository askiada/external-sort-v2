package chunksorter

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/internal/vector/key"
)

// ChunkSorter is a sorter for chunks.
type ChunkSorter struct {
	keyFn         key.AllocateKeyFn
	vectorFn      vector.AllocateVectorFnfunc
	chunkWriterFn func() model.Writer
	chunkReaderFn func(model.Writer) model.Reader

	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(
	chunkWriterFn func() model.Writer,
	chunkReaderFn func(model.Writer) model.Reader,
	keyFn key.AllocateKeyFn,
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

			row, err := rdr.Read()
			if err != nil {
				return nil, err
			}

			c.tracef("pushing row %v to buffer", row)
			err = buffer.PushBack(row)
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
	wr := c.chunkWriterFn()
	defer wr.Close()

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

	c.trace("resetting buffer")
	buffer.Reset()

	c.trace("creating chunk reader")
	chunkRdr := c.chunkReaderFn(wr)

	return chunkRdr, nil
}
