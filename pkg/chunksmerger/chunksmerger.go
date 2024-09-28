package chunksmerger

import (
	"context"
	"fmt"

	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/pkg/errors"
)

type ChunksMerger struct {
	chunkBufferSize int
	keyFn           model.AllocateKeyFn
	vectorFn        vector.AllocateVectorFnfunc
	dropDuplicates  bool

	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(
	keyFn model.AllocateKeyFn,
	vectorFn vector.AllocateVectorFnfunc,
	chunkBufferSize int,
	dropDuplicates bool,
) *ChunksMerger {
	return &ChunksMerger{
		chunkBufferSize: chunkBufferSize,
		keyFn:           keyFn,
		vectorFn:        vectorFn,
		dropDuplicates:  dropDuplicates,
	}
}

func (c *ChunksMerger) SetLogger(logger model.Logger) {
	c.defaultLoggerFields = map[string]interface{}{
		"component": "chunksMerger",
	}

	c.logger = logger
}

func (c *ChunksMerger) Merge(ctx context.Context, chunks []model.Reader, outputWriter model.Writer) (err error) {
	defer outputWriter.Close()
	c.trace("creating output buffer")
	outputBuffer := c.vectorFn(c.keyFn)

	chunkInfos := &chunkInfos{list: make([]*chunkInfo, 0, len(chunks))}

	for _, chunk := range chunks {
		c.trace("creating chunk info")
		err = chunkInfos.new(chunk, c.chunkBufferSize, c.vectorFn(c.keyFn))
		if err != nil {
			return err
		}
	}

	smallestChunk := &nextChunk{}

	c.trace("resetting order")
	chunkInfos.resetOrder()

	c.trace("creating output writer")

	for {

		if chunkInfos.len() == 0 || outputBuffer.Len() == c.chunkBufferSize {
			c.trace("writing buffer")
			err := c.writeBuffer(ctx, outputWriter, outputBuffer)
			if err != nil {
				return fmt.Errorf("can't write buffer: %w", err)
			}
		}

		if chunkInfos.len() == 0 {
			break
		}

		c.trace("getting next chunk with smallest value")
		// search the smallest value across chunk buffers by comparing first elements only
		minChunk, minIdx, err := smallestChunk.get(outputBuffer, chunkInfos, c.dropDuplicates)
		if err != nil {
			return fmt.Errorf("can't get smallest value: %w", err)
		}

		c.tracef("smaller value found in chunk %d", minIdx)

		c.trace("updating chunks")
		// remove the first element from the chunk we pulled the smallest value
		err = c.updateChunks(chunkInfos, minChunk, minIdx, c.chunkBufferSize)
		if err != nil {
			return fmt.Errorf("can't update chunks: %w", err)
		}
	}

	c.trace("resetting output buffer")
	outputBuffer.Reset()

	return nil
}

var (
	updateChunksLoggerFields = map[string]interface{}{
		"operation": "updateChunks",
	}
)

func (c *ChunksMerger) updateChunks(createdChunks *chunkInfos, minChunk *chunkInfo, minIdx, k int) error {
	c.withFieldsTracef(updateChunksLoggerFields, "front shifting buffer of chunk %d", minIdx)
	minChunk.buffer.FrontShift()

	isEmpty := false

	if minChunk.buffer.Len() == 0 {
		c.withFieldsTracef(updateChunksLoggerFields, "pulling subset from chunk %d", minIdx)
		err := minChunk.pullSubset(k)
		if err != nil {
			return fmt.Errorf("can't pull subset: %w", err)
		}

		// if after pulling data the chunk buffer is still empty then we can remove it
		if minChunk.buffer.Len() == 0 {
			isEmpty = true

			c.withFieldsTracef(updateChunksLoggerFields, "removing chunk at index %d", minIdx)
			err = createdChunks.shrink([]int{minIdx})
			if err != nil {
				return errors.Wrapf(err, "can't shrink chunk at index %d", minIdx)
			}
		}
	}
	// when we get a new element in the first chunk we need to re-order it
	if !isEmpty {
		c.withFieldsTrace(updateChunksLoggerFields, "moving first chunk to correct index")
		createdChunks.moveFirstChunkToCorrectIndex()
	}

	return nil
}

var (
	writeBufferLoggerFields = map[string]interface{}{
		"operation": "writeBuffer",
	}
)

func (c *ChunksMerger) writeBuffer(ctx context.Context, w model.Writer, rows vector.Vector) error {
	c.withFieldsTrace(writeBufferLoggerFields, "writing buffer")
	for i := range rows.Len() {
		c.withFieldsTracef(writeBufferLoggerFields, "writing row %d", i)
		err := w.WriteRow(ctx, rows.Get(i).Row)
		if err != nil {
			return errors.Wrap(err, "can't write buffer")
		}
	}

	c.withFieldsTrace(writeBufferLoggerFields, "resetting buffer")
	rows.Reset()

	return nil
}

type nextChunk struct {
	oldElem *vector.Element
}

func (nc *nextChunk) get(output vector.Vector, createdChunks *chunkInfos, dropDuplicates bool) (*chunkInfo, int, error) {
	minChunk, minValue, minIdx := createdChunks.min()
	if (!dropDuplicates || nc.oldElem == nil) || (dropDuplicates && !minValue.Key.Equal(nc.oldElem.Key)) {
		err := output.PushBack(minValue.Row)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "can't push back row %+v", minValue.Row)
		}

		nc.oldElem = minValue
	}

	return minChunk, minIdx, nil
}
