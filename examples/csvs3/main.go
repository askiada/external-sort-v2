package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/askiada/external-sort-v2/examples/s3handler"
	"github.com/askiada/external-sort-v2/internal/logger"
	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/orchestrator"
	"github.com/askiada/external-sort-v2/pkg/reader"
	"github.com/askiada/external-sort-v2/pkg/writer"
)

func sort(ctx context.Context, inputS3URL, outputS3URL string, log model.Logger) error {
	s3Cli, err := getS3Client(ctx, log)
	if err != nil {
		return fmt.Errorf("can't get s3 client: %w", err)
	}

	creatorRdrFn := func(rdr io.Reader) (model.Reader, error) {
		chunkCSVReader := csv.NewReader(rdr)

		return reader.NewSeparatedValues(chunkCSVReader, '\t')
	}

	creatorWrFn := func(wr io.WriteCloser) (model.Writer, error) {
		return writer.NewSeparatedValues(wr, '\t')
	}

	chunkRdrFn := func(idx int) (io.Reader, error) {
		chunkFile, err := os.Open(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", idx))
		if err != nil {
			return nil, fmt.Errorf("can't open chunk file: %w", err)
		}

		return chunkFile, nil
	}

	chunkWrFn := func(idx int) (io.WriteCloser, error) {
		chunkFileWriter, err := os.OpenFile(fmt.Sprintf("testdata/chunks/chunk_sorted_%d.csv", idx), os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("can't open chunk file: %w", err)
		}

		return chunkFileWriter, nil
	}

	tsvKeyFn := func(row interface{}) (model.Key, error) {
		tKey, err := key.AllocateCsv(row, 4, 7, 13)
		if err != nil {
			return nil, err
		}

		return key.AllocateUpperString(tKey.Value().(string))
	}

	orch := orchestrator.NewBasic(creatorRdrFn, creatorWrFn, chunkRdrFn, chunkWrFn, tsvKeyFn, 50, false)

	orch.SetLogger(log)

	inputS3Reader, err := s3Cli.NewReader(ctx, inputS3URL)
	if err != nil {
		return fmt.Errorf("can't create input s3 reader: %w", err)
	}

	inputCSVReader := csv.NewReader(inputS3Reader)
	inputReader, err := reader.NewSeparatedValues(inputCSVReader, '\t')
	if err != nil {
		return fmt.Errorf("can't create input reader: %w", err)
	}

	outputS3Writer, err := s3Cli.NewWriterCloser(ctx, outputS3URL)
	if err != nil {
		return fmt.Errorf("can't create output s3 writer: %w", err)
	}

	outputWriter, err := writer.NewSeparatedValues(outputS3Writer, '\t')
	if err != nil {
		return fmt.Errorf("can't create output writer: %w", err)
	}

	err = orch.Sort(ctx, inputReader, outputWriter)
	if err != nil {
		return fmt.Errorf("can't sort: %w", err)
	}

	return nil
}

func main() {
	ctx := context.Background()

	inputS3URL := "s3://test-bucket/input.csv"
	outputS3URL := "s3://test-bucket/output.csv"
	log := logger.NewLogrus()

	err := sort(ctx, inputS3URL, outputS3URL, log)
	if err != nil {
		log.Errorf("can't sort: %v", err)

		os.Exit(1)
	}
}

func getS3Client(ctx context.Context, log model.Logger) (*s3handler.Handler, error) {
	awsEnpoint := env.GetAWSEndpoint()

	awsRegion, err := env.GetAWSRegion()
	if err != nil {
		return nil, fmt.Errorf("can't get aws region: %w", err)
	}

	awsRetries, err := env.GetAWSRetries()
	if err != nil {
		return nil, fmt.Errorf("can't get aws retries: %w", err)
	}

	s3Client, err := s3handler.NewHandler(ctx, awsEnpoint, awsRegion, awsRetries, log)
	if err != nil {
		return nil, fmt.Errorf("can't create s3 client: %w", err)
	}

	return s3Client, nil
}
