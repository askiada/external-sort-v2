package orchestrator

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/model/mocks"
	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/internal/vector/key"
	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
)

type IOToReaderFn func(io.Reader) model.Reader

type IOToWriterFn func(io.WriteCloser) model.Writer

type ChunkReaderFn func(idx int) io.Reader

type ChunkWriterFn func(idx int) io.WriteCloser

type BasicOrchestrator struct {
	orch *Orchestrator
}

func NewBasic(
	rdrFn IOToReaderFn,
	wrFn IOToWriterFn,
	chunkRdrFn ChunkReaderFn,
	chunkWrFn ChunkWriterFn,
	keyFn key.AllocateKeyFn,
	chunkSize int,
	chunkMergerBufferSize int,
	dropDuplicates bool,
) *BasicOrchestrator {

	currChunkCreatorReader := 0
	m := sync.Mutex{}

	inputOffsets := []*io.PipeReader{}

	chunkCreatorReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkCreatorReader++ }()

		pr := inputOffsets[currChunkCreatorReader]
		return rdrFn(pr)
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()

		pr, pw := io.Pipe()

		inputOffsets = append(inputOffsets, pr)
		return wrFn(pw)
	}

	chunkCreator := chunkcreator.New(chunkSize, chunkCreatorReaderFn, chunkWriterCreatorFn)
	currChunkSorterReader := 0
	chunkSorterReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkSorterReader++ }()

		return rdrFn(chunkRdrFn(currChunkSorterReader))
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()

		return wrFn(chunkWrFn(currCreatorSorter))
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
