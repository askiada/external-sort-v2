package model

import "context"

// ChunkSorter is a sorter for chunks.
//
//go:generate mockery --name ChunkSorter --structname MockChunkSorter --filename chunk_sorter_mock.go
type ChunkSorter interface {
	// Sort sorts the chunks.
	Sort(ctx context.Context, rdr Reader) (Reader, error)
}
