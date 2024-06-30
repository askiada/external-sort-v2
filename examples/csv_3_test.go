package examples_test

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
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

func TestCSV3(t *testing.T) {
	/*t.Cleanup(func() {
		os.RemoveAll("testdata/chunks")
		os.Mkdir("testdata/chunks", os.ModePerm)

		os.Remove("testdata/output.csv")
		os.Remove("testdata/output_tmp.csv")
	})*/

	log := logger.NewLogrus()
	log.SetLevel("warn")

	inputFile, err := os.Open("testdata/input.csv")
	require.NoError(t, err)

	inputCSVReader := csv.NewReader(inputFile)

	inputReader := reader.NewSeparatedValues(inputCSVReader, ',')

	currChunkCreatorReader := 0
	m := sync.Mutex{}

	inputOffsets := []*io.PipeReader{}

	chunkCreatorReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkCreatorReader++ }()

		chunkCSVReader := csv.NewReader(inputOffsets[currChunkCreatorReader])

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()

		pr, pw := io.Pipe()

		inputOffsets = append(inputOffsets, pr)
		return writer.NewSeparatedValues(pw, ',')
	}

	currChunkSorterReader := 0
	chunkSorterReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()
		defer func() { currChunkSorterReader++ }()

		chunkFile, err := os.Open(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", currChunkSorterReader))
		require.NoError(t, err)

		chunkCSVReader := csv.NewReader(chunkFile)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() model.Writer {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()
		chunkFileWriter, err := os.OpenFile(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", currCreatorSorter), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		require.NoError(t, err)

		return writer.NewSeparatedValues(chunkFileWriter, ',')
	}

	chunkMergerReaderFn := func(w model.Writer) model.Reader {
		m.Lock()
		defer m.Unlock()

		chunkFile, err := os.Open("testdata/output_tmp.csv")
		require.NoError(t, err)

		chunkCSVReader := csv.NewReader(chunkFile)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	chunksWriterMerger := func() model.Writer {
		outputFile, err := os.OpenFile("testdata/output_tmp.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		require.NoError(t, err)
		return writer.NewSeparatedValues(outputFile, ',')
	}

	chunkCreator := chunkcreator.New(5, chunkCreatorReaderFn, chunkWriterCreatorFn)

	tsvKeyFn := func(row interface{}) (key.Key, error) {
		tKey, err := key.AllocateTsv(row, 1)
		if err != nil {
			return tKey, err
		}

		return key.AllocateInt(tKey.Value())
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

	outputFile, err := os.OpenFile("testdata/output.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	outputWriter := writer.NewSeparatedValues(outputFile, ',')

	err = orch.Sort(context.Background(), inputReader, outputWriter, 3, 3)
	require.NoError(t, err)

	// read output file line by line and check if it is sorted

	outputFile, err = os.Open("testdata/output.csv")
	require.NoError(t, err)

	outputScanner := bufio.NewScanner(outputFile)
	expected := []string{
		"giraffe,1",
		"test,2",
		"no idea,3",
		"miam miam,4",
		"croute,5",
		"supa,6",
		"more,7",
		"maybe,8",
		"whatever,9",
		"stuff,10",
	}

	for i := 0; outputScanner.Scan(); i++ {
		require.Equal(t, expected[i], outputScanner.Text())
	}

	require.NoError(t, outputScanner.Err())
}
