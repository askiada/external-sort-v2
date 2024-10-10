package model

import "context"

// ChunksMerger is a merger for chunks.
//
//go:generate mockery --name ChunksMerger --structname MockChunksMerger --filename chunks_merger_mock.go
type ChunksMerger interface {
	// Merge merges the chunks.
	Merge(ctx context.Context, chunks []Reader, outputWriter Writer) (err error)
	MaxMemory() int64
}
