package model

import "context"

// ChunkCreator is a creator for chunks.
//
//go:generate mockery --name ChunkCreator --structname MockChunkCreator --filename chunk_creator_mock.go
type ChunkCreator interface {
	// Create creates chunks.
	Create(ctx context.Context, input Reader, chunks chan<- Reader) error
}
