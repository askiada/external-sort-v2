package orchestrator

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/model/mocks"
)

type IOToReaderFn func(io.Reader) (model.Reader, error)

type IOToWriterFn func(io.WriteCloser) (model.Writer, error)

type ChunkReaderFn func(step string, idx int) (io.Reader, error)

type ChunkWriterFn func(step string, idx int) (io.WriteCloser, error)

type BasicOrchestrator struct {
	orch         *Orchestrator
	chunkCreator *chunkcreator.ChunkCreator
	chunkSorter  *chunksorter.ChunkSorter
	chunksMerger *chunksmerger.ChunksMerger
}

// 90% of maxMemoryBytes for Chunk creation and sorting
// 5% of maxMemoryBytes for Chunk merging
// 5% for everything else
func NewBasic(
	rdrFn IOToReaderFn,
	wrFn IOToWriterFn,
	chunkRdrFn ChunkReaderFn,
	chunkWrFn ChunkWriterFn,
	keyFn model.AllocateKeyFn,
	maxMemoryBytes int64,
	dropDuplicates bool,
) *BasicOrchestrator {

	chunkCreatorReaderFn := func(idx int) (model.Reader, error) {
		curr, err := chunkRdrFn("sort", idx)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %w", err)
		}

		rdr, err := rdrFn(curr)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}

		return rdr, err
	}

	currChunkCreatorReader := 0
	m := sync.Mutex{}

	chunkWriterCreatorFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkCreatorReader++ }()

		curr, err := chunkWrFn("sort", currChunkCreatorReader)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create chunk writer: %w", err)
		}

		wr, err := wrFn(curr)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create writer: %w", err)
		}

		return currChunkCreatorReader, wr, err
	}

	giveSomeRoomMemory := max(90*maxMemoryBytes/100, 1)

	chunkCreator := chunkcreator.New(giveSomeRoomMemory, chunkCreatorReaderFn, chunkWriterCreatorFn)
	chunkSorterReaderFn := func(idx int) (model.Reader, error) {
		curr, err := chunkRdrFn("sort", idx)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %w", err)
		}

		return rdrFn(curr)
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()

		curr, err := chunkWrFn("sort", currCreatorSorter)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create chunk writer: %w", err)
		}

		wr, err := wrFn(curr)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create writer: %w", err)
		}

		return currCreatorSorter, wr, err
	}

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkSorterReaderFn, keyFn, vector.AllocateSlice)

	giveSomeRoomMemory = max(5*maxMemoryBytes/100, 1)

	chunkMerger := chunksmerger.New(keyFn, vector.AllocateSlice, giveSomeRoomMemory, dropDuplicates)

	// TODO: add
	tracker := mocks.NewMockTracker(&testing.T{})

	orch := New(chunkCreator, chunkSorter, chunkMerger, tracker, true)

	return &BasicOrchestrator{orch, chunkCreator, chunkSorter, chunkMerger}
}

func (bo *BasicOrchestrator) SetLogger(logger model.Logger) {
	bo.chunkCreator.SetLogger(logger)
	bo.chunkSorter.SetLogger(logger)
	bo.chunksMerger.SetLogger(logger)
	bo.orch.SetLogger(logger)
}

const defaultSortConcurrency = 10

func (bo *BasicOrchestrator) Sort(ctx context.Context, inputContentLenght int64, input model.Reader, output model.Writer) error {
	return bo.orch.Sort(ctx, inputContentLenght, input, output, defaultSortConcurrency, 0)
}
