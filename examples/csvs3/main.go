package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/askiada/external-sort-v2/examples/s3handler"
	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
)

func getS3Client(ctx context.Context, log model.Logger) (*s3handler.Handler, error) {
	awsEnpoint := os.Getenv("AWS_ENDPOINT")

	awsRegion := os.Getenv("AWS_REGION")

	awsRetriesStr := os.Getenv("AWS_RETRIES")

	awsRetries, err := strconv.Atoi(awsRetriesStr)
	if err != nil {
		return nil, fmt.Errorf("can't parse aws retries: %w", err)
	}

	s3Client, err := s3handler.NewHandler(ctx, awsEnpoint, awsRegion, awsRetries, log)
	if err != nil {
		return nil, fmt.Errorf("can't create s3 client: %w", err)
	}

	return s3Client, nil
}

func sort(ctx context.Context, inputS3URL, outputS3URL string, log model.Logger) error {
	s3Cli, err := getS3Client(ctx, log)
	if err != nil {
		return fmt.Errorf("can't get s3 client: %w", err)
	}

	creatorRdrFn := func(rdr io.Reader) (model.Reader, error) {
		fields := map[string]interface{}{"component": "creator"}
		defer log.WithFieldsTrace(fields, "reader created")
		log.WithFieldsTrace(fields, "creating reader")

		chunkCSVReader := csv.NewReader(rdr)

		return reader.NewSeparatedValues(chunkCSVReader, ',')
	}

	creatorWrFn := func(wr io.WriteCloser) (model.Writer, error) {
		fields := map[string]interface{}{"component": "creator"}
		defer log.WithFieldsTrace(fields, "writer created")
		log.WithFieldsTrace(fields, "creating writer")

		return writer.NewSeparatedValues(wr, ',')
	}

	ouputBucket, outputKey, err := s3handler.Parse(outputS3URL)
	if err != nil {
		return fmt.Errorf("can't parse output s3 url: %w", err)
	}

	baseOutputNoExt := strings.TrimRight(filepath.Base(outputKey), filepath.Ext(outputKey))

	chunkRdrFn := func(step string, idx int) (io.Reader, error) {
		chunkFilename := path.Join(filepath.Dir(outputKey), fmt.Sprintf("%s_%s_%d%s", baseOutputNoExt, step, idx, filepath.Ext(outputKey)))
		chunkFile, err := s3Cli.NewReader(ctx, fmt.Sprintf("s3://%s/%s", ouputBucket, chunkFilename))

		if err != nil {
			return nil, fmt.Errorf("can't open chunk file: %w", err)
		}

		return chunkFile, nil
	}

	chunkWrFn := func(step string, idx int) (io.WriteCloser, error) {
		chunkFilename := path.Join(filepath.Dir(outputKey), fmt.Sprintf("%s_%s_%d%s", baseOutputNoExt, step, idx, filepath.Ext(outputKey)))
		chunkFileWriter, err := s3Cli.NewWriterCloser(ctx, fmt.Sprintf("s3://%s/%s", ouputBucket, chunkFilename))
		if err != nil {
			return nil, fmt.Errorf("can't open chunk file: %w", err)
		}

		return chunkFileWriter, nil
	}

	tsvKeyFn := func(row interface{}) (model.Key, error) {
		tKey, err := key.AllocateCsv(row, 1, 2)
		if err != nil {
			return nil, err
		}

		return key.AllocateUpperString(tKey.Value().(string))
	}

	orch := orchestrator.NewBasic(creatorRdrFn, creatorWrFn, chunkRdrFn, chunkWrFn, tsvKeyFn, 1_000_000, false)

	orch.SetLogger(log)

	inputS3Reader, err := s3Cli.NewReader(ctx, inputS3URL)
	if err != nil {
		return fmt.Errorf("can't create input s3 reader: %w", err)
	}

	inputCSVReader := csv.NewReader(inputS3Reader)
	inputReader, err := reader.NewSeparatedValues(inputCSVReader, ',', reader.WithSeparatedValuesHeaders(1))
	if err != nil {
		return fmt.Errorf("can't create input reader: %w", err)
	}

	outputS3Writer, err := s3Cli.NewWriterCloser(ctx, outputS3URL)
	if err != nil {
		return fmt.Errorf("can't create output s3 writer: %w", err)
	}

	outputWriter, err := writer.NewSeparatedValues(outputS3Writer, ',', writer.WithSeparatedValuesHeaders(inputReader.Headers()))
	if err != nil {
		return fmt.Errorf("can't create output writer: %w", err)
	}

	contentLength, err := s3Cli.ContentLength(ctx, inputS3URL)
	if err != nil {
		return fmt.Errorf("can't get content length: %w", err)
	}

	log.Infof("sorting %d bytes", contentLength)

	err = orch.Sort(ctx, contentLength, inputReader, outputWriter)
	if err != nil {
		return fmt.Errorf("can't sort: %w", err)
	}

	return nil
}

func main() {

	// Create a CPU profile file
	f, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	ctx := context.Background()

	inputS3URL := os.Getenv("INPUT_S3_URL")
	outputS3URL := os.Getenv("OUTPUT_S3_URL")
	log := logger.NewLogrus()

	log.SetLevel(os.Getenv("LOG_LEVEL"))

	err = sort(ctx, inputS3URL, outputS3URL, log)
	if err != nil {
		log.Errorf("can't sort: %v", err)

		os.Exit(1)
	}

	// Create a memory profile file
	memProfileFile, err := os.Create("mem.prof")
	if err != nil {
		panic(err)
	}
	defer memProfileFile.Close()

	// Write memory profile to file
	if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
		panic(err)
	}
}
