package reader

import (
	"encoding/csv"
	"errors"
	"io"

	"github.com/askiada/external-sort-v2/pkg/model"
)

type SeparatedValuesReader struct {
	row []string
	r   *csv.Reader
	err error
}

func NewSeparatedValues(r *csv.Reader, separator rune) *SeparatedValuesReader {
	s := &SeparatedValuesReader{
		r: r,
	}
	s.r.Comma = separator

	return s
}

func (s *SeparatedValuesReader) Next() bool {
	s.row, s.err = s.r.Read()
	if errors.Is(s.err, io.EOF) {
		s.err = nil
		return false
	}

	return true
}

func (s *SeparatedValuesReader) Read() (interface{}, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.row, nil
}

func (s *SeparatedValuesReader) Err() error {
	return s.err
}

var _ model.Reader = &SeparatedValuesReader{}
