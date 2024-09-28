package writer

import (
	"bufio"
	"context"

	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/pkg/errors"
)

// BufioWriter implement writer interface with a bufio writer.
type BufioWriter struct {
	w *bufio.Writer
}

// NewBufioWriter create a standard writer.
func NewBufioWriter(w *bufio.Writer) *BufioWriter {
	s := &BufioWriter{
		w: w,
	}

	return s
}

func (w *BufioWriter) WriteRow(ctx context.Context, elem interface{}) error {
	line, ok := elem.([]byte)
	if !ok {
		return errors.Errorf("can't converte interface{} to []byte: %+v", elem)
	}

	_, err := w.w.Write(line)
	if err != nil {
		return errors.Wrap(err, "can't write string")
	}

	return err
}

func (w *BufioWriter) Write(ctx context.Context, rdr model.Reader) error {

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

// Close close the bufio writer. It is the responsibility of the client to close the underlying writer.
func (w *BufioWriter) Close() error {
	err := w.w.Flush()
	if err != nil {
		return errors.Wrap(err, "can't close writer")
	}

	return nil
}

var _ model.Writer = &BufioWriter{}
