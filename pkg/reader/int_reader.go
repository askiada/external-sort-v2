package reader

import (
	"sync"

	"github.com/askiada/external-sort-v2/internal/model"
)

type IntSlice struct {
	currIdx   int
	currValue int
	Values    []int
	mu        sync.Mutex
}

func (r *IntSlice) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currIdx < len(r.Values) {
		r.currValue = r.Values[r.currIdx]
		r.currIdx++
		return true
	}

	return false
}

func (r *IntSlice) Read() (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currValue, nil
}

func (r *IntSlice) Err() error {
	return nil
}

var _ model.Reader = &IntSlice{}
