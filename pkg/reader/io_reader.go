package reader

import (
	"errors"
	"io"
)

type IO struct {
	row interface{}
	err error
	rdr io.Reader
	p   []byte
}

func New(rdr io.Reader, p []byte) *IO {
	return &IO{
		rdr: rdr,
		p:   p,
	}
}

func (r *IO) Next() bool {
	r.row, r.err = r.rdr.Read(r.p)
	if errors.Is(r.err, io.EOF) {
		r.err = nil

		return false
	}

	return true
}

func (r *IO) Read() (interface{}, error) {

	return nil, nil
}

func (r *IO) Err() error {
	return r.err
}
