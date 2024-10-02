// Package s3handler defines interactions with S3.
package s3handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

// Handler give access to the AWS config and S3 Client.
type Handler struct {
	s3Client  *s3.Client
	awsConfig aws.Config
	log       model.Logger
}

func getS3Client(cfg *aws.Config) *s3.Client {
	return s3.NewFromConfig(*cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// NewHandler create a new S3 handler.
// Endpoint must be empty to communinacte with remote S3.
func NewHandler(ctx context.Context, endpoint, region string, maxRetries int, log model.Logger) (*Handler, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(maxRetries),
	)
	if err != nil {
		return nil, fmt.Errorf("can't create aws config: %w", ErrConfig)
	}

	var awsEnpoint *string

	if endpoint != "" {
		awsEnpoint = aws.String(endpoint)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = awsEnpoint
	})

	return &Handler{awsConfig: cfg, s3Client: s3Client, log: log}, nil
}

// HeadObject get the header of an object.
func (h *Handler) HeadObject(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error) {
	res, err := h.s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("unable to head object %s %s: %w", bucket, key, err)
	}

	return res, nil
}

// Download get a file from S3 and store it on local.
func (h *Handler) Download(ctx context.Context, bucket, key string, wr io.Writer) error {
	// Create a downloader passing it the S3 client
	downloader := manager.NewDownloader(h.s3Client)
	downloader.Concurrency = 1

	_, err := downloader.Download(ctx, FakeWriterAt{wr},
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return fmt.Errorf("unable to download file %s %s: %w", bucket, key, err)
	}

	return nil
}

// Stream gets a file from S3 and stream it using a io pipe.
func (h *Handler) Stream(ctx context.Context, bucket, key string, processFn ProcessStream) error {
	headerOutput, err := h.s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("unable to head object %s %s: %w", bucket, key, err)
	}
	// Create a downloader passing it the S3 client
	downloader := manager.NewDownloader(h.s3Client)

	downloader.Concurrency = 1
	rdr, wtr := io.Pipe()

	grp, dCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer wtr.Close() //nolint:errcheck //not required

		_, err := downloader.Download(dCtx, FakeWriterAt{wtr},
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			return fmt.Errorf("unable to download file %s %s: %w", bucket, key, err)
		}

		h.log.Infof("downloaded %s %s", bucket, key)

		return nil
	})

	grp.Go(func() error {
		defer rdr.Close() //nolint:errcheck //not required

		return processFn(dCtx, rdr, *headerOutput.ContentLength)
	})

	if err := grp.Wait(); err != nil {
		return fmt.Errorf("unable to stream file %s %s: %w", bucket, key, err)
	}

	return nil
}

// FakeWriterAt defines a structure to implement io.WriteAt interface.
type FakeWriterAt struct {
	w io.Writer
}

// WriteAt implements io.WriteAt using a io.Writer.
func (fw FakeWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	n, err := fw.w.Write(p)
	if err != nil {
		return 0, fmt.Errorf("unable to write: %w", err)
	}

	return n, nil
}

// Find recursively walks through the bucket/key subtree and returns all files matching pattern.
func (h *Handler) Find(ctx context.Context, bucket, key string, pattern *regexp.Regexp) ([]types.Object, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}
	s3Pages := s3.NewListObjectsV2Paginator(h.s3Client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 1000
	})
	result := make([]types.Object, 0)

	var pageIdx int

	for s3Pages.HasMorePages() {
		pageIdx++

		// Next Page takes a new context for each page retrieval. This is where
		// you could add timeouts or deadlines.
		page, err := s3Pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get page %v, %w", pageIdx, err)
		}

		for _, obj := range page.Contents {
			if pattern == nil || pattern.MatchString(*obj.Key) {
				result = append(result, obj)
			}
		}
	}

	return result, nil
}

// UploadByte upload a slice of byte to S3.
func (h *Handler) UploadByte(ctx context.Context, bucket, key string, body []byte) error {
	_, err := h.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return fmt.Errorf("unable to put object %s %s: %w", bucket, key, err)
	}

	return nil
}

// Upload upload a file to S3.
func (h *Handler) Upload(ctx context.Context, bucket, key string, rdr io.Reader) error {
	// Create an uploader passing it the S3 client
	uploader := manager.NewUploader(h.s3Client)
	uploader.Concurrency = 1

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   rdr,
	})
	if err != nil {
		return fmt.Errorf("unable to upload object %s %s: %w", bucket, key, err)
	}

	h.log.Infof("uploaded %s %s", bucket, key)

	return nil
}

type reader struct {
	rdr io.Reader
	err error
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	return r.rdr.Read(p)
}

func (h *Handler) NewReader(ctx context.Context, surl string) (io.Reader, error) {
	s3Bucket, s3Key, err := Parse(surl)
	if err != nil {
		return nil, fmt.Errorf("unable to parse s3 url %s: %w", surl, err)
	}

	inputS3Reader, wtr := io.Pipe()

	r := &reader{rdr: inputS3Reader}

	go func() {
		defer wtr.Close()

		err := h.Download(ctx, s3Bucket, s3Key, wtr)
		if err != nil {
			r.err = fmt.Errorf("can't stream input: %w", err)
		}
	}()

	return r, nil
}

type writer struct {
	wtr io.WriteCloser
	err error
}

func (w *writer) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}

	return w.wtr.Write(p)
}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	return w.wtr.Close()
}

func (h *Handler) NewWriterCloser(ctx context.Context, surl string) (io.WriteCloser, error) {
	outputBucket, outputKey, err := Parse(surl)
	if err != nil {
		return nil, fmt.Errorf("can't parse output s3 url: %w", err)
	}

	rdr, outputS3Writer := io.Pipe()

	w := &writer{wtr: outputS3Writer}

	go func() {
		defer rdr.Close()

		err := h.Upload(ctx, outputBucket, outputKey, rdr)
		if err != nil {
			w.err = fmt.Errorf("can't upload output: %w", err)
		}
	}()

	return w, nil

}

// Parse an s3Url and return the bucket and key. Performs some basic validation
// inside.
func Parse(s3Url string) (bucket, key string, err error) { //nolint:nonamedreturns // better when more than 2 values are returned
	noPrefix := s3Url[5:]
	hasFinalSlash := strings.HasSuffix(noPrefix, "/")

	if hasFinalSlash {
		// to avoid parsing folders incorrectly
		noPrefix = strings.TrimSuffix(noPrefix, "/")
	}

	separatorIndex := strings.Index(noPrefix, "/")

	if separatorIndex == -1 {
		return "", "", fmt.Errorf("%s: %w", s3Url, ErrInvalidS3URL)
	}

	bucket = noPrefix[:separatorIndex]
	key = noPrefix[separatorIndex+1:]

	return bucket, key, nil
}

// BuildS3URL builds an s3 URL from a bucket and key.
func BuildS3URL(bucket, key string) string {
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}

var _ S3Client = &Handler{}
