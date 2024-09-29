package reader

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/askiada/external-sort-v2/pkg/model"
)

type SeparatedValuesReader struct {
	row                   []string
	headers               [][]string
	r                     *csv.Reader
	lastOffset, newOffset int64
	size                  int64
	err                   error
}

func NewSeparatedValues(r *csv.Reader, separator rune, opts ...SeparatedValuesReaderOption) (*SeparatedValuesReader, error) {
	s := &SeparatedValuesReader{
		r: r,
	}
	s.r.Comma = separator

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			return nil, fmt.Errorf("can't apply option: %w", err)
		}
	}

	return s, nil
}

func (s *SeparatedValuesReader) Next() bool {
	s.row, s.err = s.r.Read()
	if errors.Is(s.err, io.EOF) {
		s.err = nil
		return false
	}

	s.newOffset = s.r.InputOffset()
	s.size = s.newOffset - s.lastOffset

	s.lastOffset = s.newOffset

	return true
}

func (s *SeparatedValuesReader) Read() (interface{}, int64, error) {
	if s.err != nil {
		return nil, s.size, s.err
	}

	return s.row, s.size, nil
}

func (s *SeparatedValuesReader) Headers() [][]string {
	return s.headers
}

func (s *SeparatedValuesReader) Err() error {
	return s.err
}

var _ model.Reader = &SeparatedValuesReader{}

type SeparatedValuesReaderOption func(s *SeparatedValuesReader) error

func WithLazyQuotes(lazyQuotes bool) SeparatedValuesReaderOption {
	return func(s *SeparatedValuesReader) error {
		s.r.LazyQuotes = lazyQuotes

		return nil
	}
}

func WithSeparatedValuesHeaders(numRows int) SeparatedValuesReaderOption {
	return func(s *SeparatedValuesReader) error {
		for range numRows {
			row, err := s.r.Read()
			if err != nil {
				return fmt.Errorf("can't read headers: %w", err)
			}

			s.headers = append(s.headers, row)
		}

		return nil
	}
}
