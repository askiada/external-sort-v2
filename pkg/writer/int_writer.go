package writer

import (
	"context"
	"sync"

	"github.com/askiada/external-sort-v2/pkg/model"
)

type IntSlice struct {
	Values []int
	mu     sync.Mutex
}

func (w *IntSlice) Write(ctx context.Context, rdr model.Reader) error {

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

func (w *IntSlice) WriteRow(ctx context.Context, row interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.Values = append(w.Values, row.(int))
	return nil
}

func (w *IntSlice) Close() error {
	return nil
}

var _ model.Writer = &IntSlice{}
