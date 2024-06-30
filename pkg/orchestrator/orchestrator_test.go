package orchestrator_test

import (
	"context"
	"testing"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/model/mocks"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestOrchestratorNilInput(t *testing.T) {
	t.Parallel()

	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	err := orch.Sort(context.Background(), nil, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrNilInput)
}

func TestOrchestratorNilOutput(t *testing.T) {
	t.Parallel()
	inputMock := mocks.NewMockReader(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	err := orch.Sort(context.Background(), inputMock, nil, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrNilOutput)
}

func TestOrchestratorNilChunkCreator(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(nil, chunkSorterMock, chunksMergerMock, trackerMock, false)

	err := orch.Sort(context.Background(), inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrNilChunkCreator)
}

func TestOrchestratorNilChunkSorter(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, nil, chunksMergerMock, trackerMock, false)

	err := orch.Sort(context.Background(), inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrorNilChunkSorter)
}

func TestOrchestratorNilChunksMerger(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, nil, trackerMock, false)

	err := orch.Sort(context.Background(), inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrorNilChunksMerger)
}

func TestOrchestratorNilTracker(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, nil, false)

	err := orch.Sort(context.Background(), inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrorNilTracker)
}

func TestOrchestratorChunkCreatorNilChunk(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- nil
	})

	err := orch.Sort(context.Background(), inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrNilChunk)
}

func TestOrchestratorChunkSorterError(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(nil, assert.AnError)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrFailedToSortChunk)
}

func TestOrchestratorChunkSorterNilChunk(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)
	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(nil, nil)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrNilChunk)
}

func TestOrchestratorChunkMergerError(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrFailedToMergeChunks)
}

func TestOrchestratorOutputCloseError(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(assert.AnError)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, orchestrator.ErrFailedToCloseOutput)
}

func TestOrchestratorOutput(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(nil)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.NoError(t, err)
}

func TestOrchestratorOutputNegativeMaxChunkSorter(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(nil)

	err := orch.Sort(ctx, inputMock, outputMock, -1, 0)
	require.NoError(t, err)
}

func TestOrchestratorOutputNegativeMaxChunkMerger(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(nil)

	err := orch.Sort(ctx, inputMock, outputMock, 0, -1)
	require.NoError(t, err)
}

func TestOrchestratorReadInputContextCancel(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx, cancel := context.WithCancel(context.Background())

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
		cancel()
	})

	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil).Maybe()
	outputMock.On("Close").Return(nil).Maybe()

	err := orch.Sort(ctx, inputMock, outputMock, 0, -1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestOrchestratorSortContextCancel(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)
	chunkReader := mocks.NewMockReader(t)

	ctx, cancel := context.WithCancel(context.Background())

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})

	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil).Run(func(args mock.Arguments) {
		cancel()
	})
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil).Maybe()
	outputMock.On("Close").Return(nil).Maybe()

	err := orch.Sort(ctx, inputMock, outputMock, 0, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestOrchestratorOutputMaxChunkMerger3(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
		args.Get(2).(chan<- model.Reader) <- chunkReader
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(nil)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 3)
	require.NoError(t, err)
}

func TestOrchestratorOutputMaxChunkMerger2(t *testing.T) {
	t.Parallel()

	inputMock := mocks.NewMockReader(t)
	outputMock := mocks.NewMockWriter(t)

	chunkSorterMock := mocks.NewMockChunkSorter(t)
	chunksMergerMock := mocks.NewMockChunksMerger(t)
	chunkCreatorMock := mocks.NewMockChunkCreator(t)
	trackerMock := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreatorMock, chunkSorterMock, chunksMergerMock, trackerMock, false)

	chunkReader := mocks.NewMockReader(t)

	ctx := context.Background()

	chunkCreatorMock.On("Create", mock.Anything, inputMock, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		args.Get(2).(chan<- model.Reader) <- chunkReader
		args.Get(2).(chan<- model.Reader) <- chunkReader
		args.Get(2).(chan<- model.Reader) <- chunkReader
	})
	chunkSorterMock.On("Sort", mock.Anything, chunkReader).Return(mocks.NewMockReader(t), nil)
	chunksMergerMock.On("Merge", mock.Anything, mock.Anything, outputMock).Return(nil)

	outputMock.On("Close").Return(nil)

	err := orch.Sort(ctx, inputMock, outputMock, 0, 2)
	require.NoError(t, err)
}
