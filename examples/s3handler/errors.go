package s3handler

// s3handlerError is used to define constant errors.
type s3handlerError string

// Error implements the error interface.
func (o s3handlerError) Error() string {
	return string(o)
}

// ErrInvalidS3URL is returned when an S3 URL is invalid.
const (
	ErrInvalidS3URL s3handlerError = "invalid s3 url"
	ErrConfig       s3handlerError = "can't create aws config"
)
