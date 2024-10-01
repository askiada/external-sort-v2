package examples_test

import (
	"context"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/model/mocks"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
	"github.com/stretchr/testify/require"
)

func TestIntSlice(t *testing.T) {
	log := logger.NewLogrus()
	log.SetLevel("trace")
	chunkWritersCreator := []*writer.IntSlice{
		{}, //chunk 1
		{}, //chunk 2
	}

	chunkWritersSorter := []*writer.IntSlice{
		{}, //chunk 1
		{}, //chunk 2
	}

	chunkReaderFn := func(w model.Writer) (model.Reader, error) {
		return &reader.IntSlice{Values: w.(*writer.IntSlice).Values}, nil
	}

	currCreatorWriter := 0

	m := sync.Mutex{}

	chunkWriterCreatorFn := func() (model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()
		return chunkWritersCreator[currCreatorWriter], nil
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() (model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()
		return chunkWritersSorter[currCreatorSorter], nil
	}

	chunkCreator := chunkcreator.New(40, chunkReaderFn, chunkWriterCreatorFn)

	intKeyFn := key.AllocateInt
	vectorFn := vector.AllocateSlice

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkReaderFn, intKeyFn, vectorFn)

	chunksMerger := chunksmerger.New(intKeyFn, vectorFn, 16, false)

	tracker := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreator, chunkSorter, chunksMerger, tracker, true)
	orch.SetLogger(log)
	chunkCreator.SetLogger(log)
	chunkSorter.SetLogger(log)
	chunksMerger.SetLogger(log)

	inputReader := &reader.IntSlice{Values: []int{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}}

	outputWriter := &writer.IntSlice{}

	err := orch.Sort(context.Background(), inputReader, outputWriter, 1, 1)
	require.NoError(t, err)

	require.Equal(t, []int{10, 1, 9, 2, 8}, chunkWritersCreator[0].Values)
	require.Equal(t, []int{3, 7, 4, 6, 5}, chunkWritersCreator[1].Values)

	require.Equal(t, []int{1, 2, 8, 9, 10}, chunkWritersSorter[0].Values)
	require.Equal(t, []int{3, 4, 5, 6, 7}, chunkWritersSorter[1].Values)

	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, outputWriter.Values)
}
