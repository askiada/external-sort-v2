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

	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/askiada/external-sort-v2/pkg/model"
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

	creatorRdrFn := func(rdr io.Reader) (model.Reader, error) {
		chunkCSVReader := csv.NewReader(rdr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	creatorWrFn := func(wr io.WriteCloser) (model.Writer, error) {
		return writer.NewSeparatedValues(wr, ',')
	}

	chunkRdrFn := func(step string, idx int) (io.Reader, error) {
		chunkFile, err := os.Open(fmt.Sprintf("testdata/chunks/chunk_sorted_%s_%d.csv", step, idx))
		if err != nil {
			return nil, err
		}

		return chunkFile, nil
	}

	chunkWrFn := func(step string, idx int) (io.WriteCloser, error) {
		chunkFileWriter, err := os.OpenFile(
			fmt.Sprintf("testdata/chunks/chunk_sorted_%s_%d.csv", step, idx),
			os.O_CREATE|os.O_TRUNC|os.O_RDWR,
			os.ModePerm,
		)
		if err != nil {
			return nil, err
		}

		return chunkFileWriter, nil
	}

	tsvKeyFn := func(row interface{}) (model.Key, error) {
		tKey, err := key.AllocateCsv(row, 1)
		if err != nil {
			return tKey, err
		}

		return key.AllocateInt(tKey.Value())
	}

	orch := orchestrator.NewBasic(creatorRdrFn, creatorWrFn, chunkRdrFn, chunkWrFn, tsvKeyFn, 5, false)

	orch.SetLogger(log)
	ctx := context.Background()
	inputFile, err := os.Open("testdata/input.csv")
	require.NoError(t, err)

	inputCSVReader := csv.NewReader(inputFile)

	inputReader, err := reader.NewSeparatedValues(inputCSVReader, ',', reader.WithSeparatedValuesHeaders(1))
	require.NoError(t, err)

	outputFile, err := os.OpenFile("testdata/output.csv", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	require.NoError(t, err)
	outputWriter, err := writer.NewSeparatedValues(outputFile, ',', writer.WithSeparatedValuesHeaders(inputReader.Headers()))
	require.NoError(t, err)

	err = orch.Sort(ctx, inputReader, outputWriter)
	require.NoError(t, err)

	outputFile, err = os.Open("testdata/output.csv")
	require.NoError(t, err)

	outputScanner := bufio.NewScanner(outputFile)
	expected := []string{
		"name,count",
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
