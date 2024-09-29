package model

// Reader define a basic reader.
//
//go:generate mockery --name Reader --structname MockReader --filename reader_mock.go
type Reader interface {
	Next() bool
	Read() (interface{}, int64, error)
	Err() error
}
