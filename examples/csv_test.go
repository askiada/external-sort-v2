package examples_test

/*
import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/model/mocks"
	"github.com/askiada/external-sort-v2/internal/vector"
	"github.com/askiada/external-sort-v2/internal/vector/key"
	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
	"github.com/stretchr/testify/require"
)

func TestCSV(t *testing.T) {

	log := logger.NewLogrus()
	log.SetLevel("trace")

	chunkBufferCreator := []*bytes.Buffer{
		{},
		{},
	}

	chunkWritersCreator := []*writer.SeparatedValuesWriter{
		writer.NewSeparatedValues(chunkBufferCreator[0], ','),
		writer.NewSeparatedValues(chunkBufferCreator[1], ','),
	}

	chunkBufferSorter := []*bytes.Buffer{
		{},
		{},
	}

	chunkWritersSorter := []*writer.SeparatedValuesWriter{
		writer.NewSeparatedValues(chunkBufferSorter[0], ','),
		writer.NewSeparatedValues(chunkBufferSorter[1], ','),
	}

	chunkBufferMerger := &bytes.Buffer{}
	chunkWritersMerger := writer.NewSeparatedValues(chunkBufferMerger, ',')


		chunkCreatorBufferReader := []io.Reader{
			&bytes.Buffer{},
			&bytes.Buffer{},
		}


	currChunkCreatorReader := 0
	m := sync.Mutex{}

	chunkCreatorReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkCreatorReader++ }()

		pr, pw := io.Pipe()
		go func() {
			// It's important to close the writer when the copy is done to signal EOF to the reader
			defer pw.Close()

			// Copy the data from the buffer to the writer
			n, err := io.Copy(pw, chunkBufferCreator[currChunkCreatorReader])
			require.NoError(t, err)

			log.Tracef("copied %d bytes", n)
		}()

		chunkCSVReader := csv.NewReader(pr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

		chunkSorterBufferReader := []io.Reader{
			&bytes.Buffer{},
			&bytes.Buffer{},
		}

	currChunkSorterReader := 0

	chunkSorterReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkSorterReader++ }()
		pr, pw := io.Pipe()
		go func() {
			// It's important to close the writer when the copy is done to signal EOF to the reader
			defer pw.Close()

			// Copy the data from the buffer to the writer
			_, err := io.Copy(pw, chunkBufferSorter[currChunkSorterReader])
			require.NoError(t, err)
		}()

		chunkCSVReader := csv.NewReader(pr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	chunkMergerBufferReader := &bytes.Buffer{}

	currChunkMergerReader := 0

	chunksWriterMerger := func() model.Writer {
		return chunkWritersMerger
	}

	chunkMergerReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkMergerReader++ }()

		_, err := io.Copy(chunkBufferMerger, chunkMergerBufferReader)
		require.NoError(t, err)

		chunkCSVReader := csv.NewReader(chunkMergerBufferReader)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()
		return chunkWritersCreator[currCreatorWriter]
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()
		return chunkWritersSorter[currCreatorSorter]
	}

	chunkCreator := chunkcreator.New(5, chunkCreatorReaderFn, chunkWriterCreatorFn)

	tsvKeyFn := func(row interface{}) (key.Key, error) {
		return key.AllocateTsv(row, 0)
	}

	vectorFn := vector.AllocateSlice

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkSorterReaderFn, tsvKeyFn, vectorFn)

	chunksMerger := chunksmerger.New(chunksWriterMerger, chunkMergerReaderFn, tsvKeyFn, vectorFn, 2, false)

	tracker := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreator, chunkSorter, chunksMerger, tracker)

	orch.SetLogger(log)
	chunkCreator.SetLogger(log)
	chunkSorter.SetLogger(log)
	chunksMerger.SetLogger(log)

	inputReader := reader.NewSeparatedValues(csv.NewReader(bytes.NewBufferString("10,1\n9,2\n8,3\n7,4\n6,5\n")), ',')

	outputWriter := &writer.IntSlice{}

	err := orch.Sort(context.Background(), inputReader, outputWriter, 3, 3)
	require.NoError(t, err)

		require.Equal(t, []int{10, 1, 9, 2, 8}, chunkWritersCreator[0].Values)
		require.Equal(t, []int{3, 7, 4, 6, 5}, chunkWritersCreator[1].Values)

		require.Equal(t, []int{1, 2, 8, 9, 10}, chunkWritersSorter[0].Values)
		require.Equal(t, []int{3, 4, 5, 6, 7}, chunkWritersSorter[1].Values)

	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, outputWriter.Values)
}
*/
