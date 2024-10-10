package s3handler

import (
	"context"
	"io"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ProcessStream defines function to process a file stream.
type ProcessStream func(ctx context.Context, r *io.PipeReader, contentLength int64) error

// S3Client contains everything needed to interact with S3.
//
//go:generate mockery --name S3Client --structname MockS3Client --filename s3_client_mock.go
type S3Client interface {
	// Stream returns a reader for the given key.
	Stream(ctx context.Context, bucket string, key string, processFn ProcessStream) error
	// Upload uploads the given reader to the given key.
	Upload(ctx context.Context, bucket, key string, rdr io.Reader) error

	// UploadByte uploads the given byte array to the given key.
	UploadByte(ctx context.Context, bucket, key string, body []byte) error

	// HeadObject returns the metadata of the given key.
	HeadObject(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error)

	// Find returns all objects that match the given pattern.
	Find(ctx context.Context, bucket, key string, pattern *regexp.Regexp) ([]types.Object, error)

	// Download downloads the given key to the given writer.
	Download(ctx context.Context, bucket, key string, wr io.Writer) error
}

// File stores information about a file.
type S3File struct {
	LastModified *time.Time
	Path         string
}
