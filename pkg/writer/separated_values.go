package writer

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/pkg/errors"
)

type SeparatedValuesWriter struct {
	origWriter io.WriteCloser
	w          *csv.Writer
	headers    [][]string
}

func NewSeparatedValues(w io.WriteCloser, separator rune, opts ...SeparatedValuesWriterOption) (*SeparatedValuesWriter, error) {
	s := &SeparatedValuesWriter{
		origWriter: w,
		w:          csv.NewWriter(w),
	}
	s.w.Comma = separator

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			return nil, fmt.Errorf("can't apply option: %w", err)
		}
	}

	for _, row := range s.headers {
		err := s.w.Write(row)
		if err != nil {
			return nil, errors.Wrap(err, "can't write headers")
		}

	}

	return s, nil
}

func (s *SeparatedValuesWriter) WriteRow(ctx context.Context, elem interface{}) error {
	line, ok := elem.([]string)
	if !ok {
		return errors.Errorf("can't converte interface{} to []string: %+v", elem)
	}

	err := s.w.Write(line)
	if err != nil {
		return errors.Wrap(err, "can't write line")
	}

	return nil
}

func (w *SeparatedValuesWriter) Write(ctx context.Context, rdr model.Reader) error {
	for rdr.Next() {
		val, _, err := rdr.Read()
		if err != nil {
			return err
		}
		err = w.WriteRow(ctx, val)
		if err != nil {
			return err
		}
	}

	if err := rdr.Err(); err != nil {
		return err
	}

	return nil
}

func (s *SeparatedValuesWriter) Close() error {
	defer s.origWriter.Close()

	s.w.Flush()

	if s.w.Error() != nil {
		return errors.Wrap(s.w.Error(), "can't close writer")
	}

	return nil
}

var _ model.Writer = &SeparatedValuesWriter{}

type SeparatedValuesWriterOption func(*SeparatedValuesWriter) error

func WithSeparatedValuesHeaders(headers [][]string) SeparatedValuesWriterOption {
	return func(s *SeparatedValuesWriter) error {
		s.headers = headers
		return nil
	}
}
