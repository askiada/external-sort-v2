package reader

import (
	"sync"

	"github.com/askiada/external-sort-v2/pkg/model"
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

func (r *IntSlice) Read() (interface{}, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// It should be 4 bytes for each int with x32 architecture
	return r.currValue, 8, nil
}

func (r *IntSlice) Err() error {
	return nil
}

var _ model.Reader = &IntSlice{}
