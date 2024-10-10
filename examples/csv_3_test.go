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

func TestCSV3(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("testdata/chunks")
		os.Mkdir("testdata/chunks", os.ModePerm)

		os.Remove("testdata/output.csv")
		os.Remove("testdata/output_tmp.csv")
	})

	log := logger.NewLogrus()
	log.SetLevel("trace")

	inputFile, err := os.Open("testdata/input.csv")
	require.NoError(t, err)

	inputCSVReader := csv.NewReader(inputFile)

	inputReader, err := reader.NewSeparatedValues(inputCSVReader, ',')
	require.NoError(t, err)

	m := sync.Mutex{}

	inputOffsets := []*io.PipeReader{}

	chunkCreatorReaderFn := func(idx int) (model.Reader, error) {
		m.Lock()
		defer m.Unlock()

		pr := inputOffsets[idx]

		chunkCSVReader := csv.NewReader(pr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorWriter := 0

	chunkWriterCreatorFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorWriter++ }()

		pr, pw := io.Pipe()

		inputOffsets = append(inputOffsets, pr)
		wr, err := writer.NewSeparatedValues(pw, ',')
		require.NoError(t, err)

		return currCreatorWriter, wr, nil
	}
	chunkSorterReaderFn := func(idx int) (model.Reader, error) {

		chunkFile, err := os.Open(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", idx))
		require.NoError(t, err)

		chunkCSVReader := csv.NewReader(chunkFile)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	currCreatorSorter := 0

	chunkWriterSorterrFn := func() (int, model.Writer, error) {
		m.Lock()
		defer m.Unlock()
		defer func() { currCreatorSorter++ }()
		chunkFileWriter, err := os.OpenFile(
			fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", currCreatorSorter),
			os.O_CREATE|os.O_TRUNC|os.O_RDWR,
			os.ModePerm,
		)
		require.NoError(t, err)

		wr, err := writer.NewSeparatedValues(chunkFileWriter, ',')
		require.NoError(t, err)

		return currCreatorSorter, wr, nil
	}

	chunkCreator := chunkcreator.New(5, chunkCreatorReaderFn, chunkWriterCreatorFn)

	tsvKeyFn := func(row interface{}) (model.Key, error) {
		tKey, err := key.AllocateCsv(row, 1)
		if err != nil {
			return tKey, err
		}

		return key.AllocateInt(tKey.Value())
	}

	vectorFn := vector.AllocateSlice

	chunkSorter := chunksorter.New(chunkWriterSorterrFn, chunkSorterReaderFn, tsvKeyFn, vectorFn)

	chunksMerger := chunksmerger.New(tsvKeyFn, vectorFn, 2, false)

	tracker := mocks.NewMockTracker(t)

	orch := orchestrator.New(chunkCreator, chunkSorter, chunksMerger, tracker, false)

	orch.SetLogger(log)
	chunkCreator.SetLogger(log)
	chunkSorter.SetLogger(log)
	chunksMerger.SetLogger(log)

	outputFile, err := os.OpenFile("testdata/output.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	require.NoError(t, err)
	outputWriter, err := writer.NewSeparatedValues(outputFile, ',')
	require.NoError(t, err)

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
