package writer

import (
	"context"
	"encoding/csv"
	"io"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/pkg/errors"
)

type SeparatedValuesWriter struct {
	w *csv.Writer
}

func NewSeparatedValues(w io.Writer, separator rune) *SeparatedValuesWriter {
	s := &SeparatedValuesWriter{
		w: csv.NewWriter(w),
	}
	s.w.Comma = separator

	return s
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
		val, err := rdr.Read()
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
	s.w.Flush()

	if s.w.Error() != nil {
		return errors.Wrap(s.w.Error(), "can't close writer")
	}

	return nil
}

var _ model.Writer = &SeparatedValuesWriter{}
