package chunksmerger

import (
	"fmt"
	"sort"

	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/pkg/errors"
)

// chunkInfo define a chunk.
type chunkInfo struct {
	reader model.Reader
	buffer vector.Vector
}

// pullSubset Add to vector the specified number of elements.
// It stops if there is no elements left to add.
func (c *chunkInfo) pullSubset(size int) (err error) {
	elemIdx := 0
	for elemIdx < size && c.reader.Next() {
		row, err := c.reader.Read()
		if err != nil {
			return fmt.Errorf("can't read row: %w", err)
		}

		err = c.buffer.PushBack(row)
		if err != nil {
			return fmt.Errorf("can't push row to buffer: %w", err)
		}

		elemIdx++
	}

	if c.reader.Err() != nil {
		return errors.Wrap(c.reader.Err(), "chunk reader encountered an error")
	}

	return nil
}

// chunkInfos Pull of chunkInfos.
type chunkInfos struct {
	list []*chunkInfo
}

// new Create a new chunk and initialise it.
func (c *chunkInfos) new(rder model.Reader, size int, buffer vector.Vector) error {

	elem := &chunkInfo{
		reader: rder,
		buffer: buffer,
	}

	err := elem.pullSubset(size)
	if err != nil {
		return fmt.Errorf("can't pull subset: %w", err)
	}

	c.list = append(c.list, elem)

	return nil
}

// shrink Remove all the chunks at the specified indexes
// it removes the local file created and close the file descriptor.
func (c *chunkInfos) shrink(toShrink []int) error {
	for i, shrinkIndex := range toShrink {
		shrinkIndex -= i

		// we want to preserve order
		c.list = append(c.list[:shrinkIndex], c.list[shrinkIndex+1:]...)
	}

	return nil
}

// len total number of chunks.
func (c *chunkInfos) len() int {
	return len(c.list)
}

// moveFirstChunkToCorrectIndex Check where the first chunk should using the first value in the buffer.
func (c *chunkInfos) moveFirstChunkToCorrectIndex() {
	elem := c.list[0]
	c.list = c.list[1:]
	pos := sort.Search(len(c.list), func(i int) bool {
		return !vector.Less(c.list[i].buffer.Get(0), elem.buffer.Get(0))
	})
	// TODO: c.list = c.list[1:] and the following line create an unnecessary allocation.
	c.list = append(c.list[:pos], append([]*chunkInfo{elem}, c.list[pos:]...)...)
}

// min Check all the first elements of all the chunks and returns the smallest value.
func (c *chunkInfos) min() (minChunk *chunkInfo, minValue *vector.Element, minIdx int) {
	minValue = c.list[0].buffer.Get(0)
	minIdx = 0
	minChunk = c.list[0]

	return minChunk, minValue, minIdx
}

// resetOrder Put all the chunks in ascending order
// Compare the first element of each chunk.
func (c *chunkInfos) resetOrder() {
	if len(c.list) > 1 {
		sort.Slice(c.list, func(i, j int) bool {
			return vector.Less(c.list[i].buffer.Get(0), c.list[j].buffer.Get(0))
		})
	}
}
