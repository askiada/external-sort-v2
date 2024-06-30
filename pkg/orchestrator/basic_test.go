package orchestrator_test

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/vector/key"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
	"github.com/stretchr/testify/require"
)

func TestBasicOrchestrator(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("testdata/chunks")
		os.Mkdir("testdata/chunks", os.ModePerm)

		os.Remove("testdata/output.csv")
		os.Remove("testdata/output_tmp.csv")
	})

	log := logger.NewLogrus()
	log.SetLevel("trace")

	creatorRdrFn := func(rdr io.Reader) model.Reader {
		chunkCSVReader := csv.NewReader(rdr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	creatorWrFn := func(wr io.WriteCloser) model.Writer {
		return writer.NewSeparatedValues(wr, ',')
	}

	chunkRdrFn := func(idx int) io.Reader {
		chunkFile, err := os.Open(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", idx))
		require.NoError(t, err)

		return chunkFile
	}

	chunkWrFn := func(idx int) io.WriteCloser {
		chunkFileWriter, err := os.OpenFile(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", idx), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		require.NoError(t, err)

		return chunkFileWriter
	}

	tsvKeyFn := func(row interface{}) (key.Key, error) {
		tKey, err := key.AllocateCsv(row, 1)
		if err != nil {
			return tKey, err
		}

		return key.AllocateInt(tKey.Value())
	}

	orch := orchestrator.NewBasic(creatorRdrFn, creatorWrFn, chunkRdrFn, chunkWrFn, tsvKeyFn, 5, 5, false)

	orch.SetLogger(log)
	ctx := context.Background()
	inputFile, err := os.Open("testdata/input.csv")
	require.NoError(t, err)

	inputCSVReader := csv.NewReader(inputFile)

	inputReader := reader.NewSeparatedValues(inputCSVReader, ',')

	outputFile, err := os.OpenFile("testdata/output.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	require.NoError(t, err)
	outputWriter := writer.NewSeparatedValues(outputFile, ',')

	err = orch.Sort(ctx, inputReader, outputWriter, 3, 3)
	require.NoError(t, err)

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
