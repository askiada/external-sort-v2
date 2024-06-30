package reader

import (
	"bufio"
)

type BufioScanner struct {
	row   interface{}
	err   error
	rdr   bufio.Scanner
	delim byte
}

func NewBufioScanner(rdr bufio.Scanner) *BufioScanner {
	return &BufioScanner{
		rdr: rdr,
	}
}

func (r *BufioScanner) Next() bool {
	next := r.rdr.Scan()

	return next
}

func (r *BufioScanner) Read() (interface{}, error) {
	return r.rdr.Bytes(), nil
}

func (r *BufioScanner) Err() error {
	return r.rdr.Err()
}
