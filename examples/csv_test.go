package examples_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/pkg/key"

	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/model/mocks"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
	"github.com/stretchr/testify/require"
)

type Buffer struct {
	*bytes.Buffer
}

func (b *Buffer) Close() error {
	return nil
}

func TestCSV(t *testing.T) {

	log := logger.NewLogrus()
	log.SetLevel("trace")

	chunkBufferCreator := []*Buffer{
		{
			Buffer: &bytes.Buffer{},
		},
		{
			Buffer: &bytes.Buffer{},
		},
	}

	chWr0, err := writer.NewSeparatedValues(chunkBufferCreator[0], ',')
	require.NoError(t, err)

	chWr1, err := writer.NewSeparatedValues(chunkBufferCreator[1], ',')
	require.NoError(t, err)

	chunkWritersCreator := []*writer.SeparatedValuesWriter{
		chWr0,
		chWr1,
	}

	chunkBufferSorter := []*Buffer{
		{
			Buffer: &bytes.Buffer{},
		},
		{
			Buffer: &bytes.Buffer{},
		},
	}

	chWrSort0, err := writer.NewSeparatedValues(chunkBufferSorter[0], ',')
	require.NoError(t, err)

	chWrSort1, err := writer.NewSeparatedValues(chunkBufferSorter[1], ',')
	require.NoError(t, err)

	chunkWritersSorter := []*writer.SeparatedValuesWriter{
		chWrSort0,
		chWrSort1,
	}

	m := sync.Mutex{}

	chunkCreatorReaderFn := func(idx int) (model.Reader, error) {

		pr, pw := io.Pipe()
		go func() {
			// It's important to close the writer when the copy is done to signal EOF to the reader
			defer pw.Close()

			// Copy the data from the buffer to the writer
			n, err := io.Copy(pw, chunkBufferCreator[idx])
			require.NoError(t, err)

			log.Tracef("copied %d bytes", n)
		}()

		chunkCSVReader := csv.NewReader(pr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	chunkSorterReaderFn := func(idx int) (model.Reader, error) {
		pr, pw := io.Pipe()
		go func() {
			// It's important to close the writer when the copy is done to signal EOF to the reader
			defer pw.Close()

			// Copy the data from the buffer to the writer
			_, err := io.Copy(pw, chunkBufferSorter[idx])
			require.NoError(t, err)
		}()

		chunkCSVReader := csv.NewReader(pr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()
		return currCreatorWriter, chunkWritersCreator[currCreatorWriter], nil
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()
		return currCreatorSorter, chunkWritersSorter[currCreatorSorter], nil
	}

	chunkCreator := chunkcreator.New(40, chunkCreatorReaderFn, chunkWriterCreatorFn)

	tsvKeyFn := func(row interface{}) (model.Key, error) {
		return key.AllocateCsv(row, 0)
	}

	vectorFn := vector.AllocateSlice

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkSorterReaderFn, tsvKeyFn, vectorFn)

	chunksMerger := chunksmerger.New(tsvKeyFn, vectorFn, 8, false)

	tracker := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreator, chunkSorter, chunksMerger, tracker, true)

	orch.SetLogger(log)
	chunkCreator.SetLogger(log)
	chunkSorter.SetLogger(log)
	chunksMerger.SetLogger(log)

	inputReader, err := reader.NewSeparatedValues(csv.NewReader(bytes.NewBufferString("10,1\n9,2\n8,3\n7,4\n6,5\n")), ',')
	require.NoError(t, err)

	outputWriter := &writer.IntSlice{}

	err = orch.Sort(context.Background(), inputReader, outputWriter, 2, 0)
	require.NoError(t, err)
	/*
		require.Equal(t, []int{10, 1, 9, 2, 8}, chunkWritersCreator[0].Values)
		require.Equal(t, []int{3, 7, 4, 6, 5}, chunkWritersCreator[1].Values)

		require.Equal(t, []int{1, 2, 8, 9, 10}, chunkWritersSorter[0].Values)
		require.Equal(t, []int{3, 4, 5, 6, 7}, chunkWritersSorter[1].Values)
	*/
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, outputWriter.Values)
}
