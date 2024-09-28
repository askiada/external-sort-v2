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

type ChunkReaderFn func(idx int) (io.Reader, error)

type ChunkWriterFn func(idx int) (io.WriteCloser, error)

type BasicOrchestrator struct {
	orch *Orchestrator
}

func NewBasic(
	rdrFn IOToReaderFn,
	wrFn IOToWriterFn,
	chunkRdrFn ChunkReaderFn,
	chunkWrFn ChunkWriterFn,
	keyFn model.AllocateKeyFn,
	chunkSize int,
	chunkMergerBufferSize int,
	dropDuplicates bool,
) *BasicOrchestrator {

	currChunkCreatorReader := 0
	m := sync.Mutex{}

	inputOffsets := []*io.PipeReader{}

	chunkCreatorReaderFn := func(w model.Writer) (model.Reader, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkCreatorReader++ }()

		pr := inputOffsets[currChunkCreatorReader]
		return rdrFn(pr)
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() (model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()

		pr, pw := io.Pipe()

		inputOffsets = append(inputOffsets, pr)
		return wrFn(pw)
	}

	chunkCreator := chunkcreator.New(chunkSize, chunkCreatorReaderFn, chunkWriterCreatorFn)
	currChunkSorterReader := 0
	chunkSorterReaderFn := func(w model.Writer) (model.Reader, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkSorterReader++ }()

		curr, err := chunkRdrFn(currChunkSorterReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk reader: %w", err)
		}

		return rdrFn(curr)
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() (model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()

		curr, err := chunkWrFn(currCreatorSorter)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk writer: %w", err)
		}

		return wrFn(curr)
	}

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkSorterReaderFn, keyFn, vector.AllocateSlice)

	chunkMerger := chunksmerger.New(keyFn, vector.AllocateSlice, chunkMergerBufferSize, dropDuplicates)

	// TODO: add
	tracker := mocks.NewMockTracker(&testing.T{})

	orch := New(chunkCreator, chunkSorter, chunkMerger, tracker, false)

	return &BasicOrchestrator{orch}
}

func (bo *BasicOrchestrator) SetLogger(logger model.Logger) {
	bo.orch.SetLogger(logger)
}

func (bo *BasicOrchestrator) Sort(ctx context.Context, input model.Reader, output model.Writer, maxChunkSorter int, maxChunkMerger int) error {
	return bo.orch.Sort(ctx, input, output, maxChunkSorter, maxChunkMerger)
}
