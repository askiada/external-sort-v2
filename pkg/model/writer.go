package model

import "context"

// Writer is a writer.
//
//go:generate mockery --name Writer --structname MockWriter --filename writer_mock.go
type Writer interface {
	WriteRow(ctx context.Context, row interface{}) (err error)
	Close() (err error)
}
