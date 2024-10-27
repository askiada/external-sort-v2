package vector

import (
	"sort"

	"github.com/askiada/external-sort-v2/pkg/model"
)

var _ Vector = &SliceVec{}

func AllocateSlice(allocateKey func(row interface{}) (model.Key, error)) Vector {
	return &SliceVec{
		allocateKey: allocateKey,
	}
}

type SliceVec struct {
	totalSize   int64
	allocateKey func(row interface{}) (model.Key, error)
	s           []*Element
}

func (v *SliceVec) Reset() {
	v.totalSize = 0
	v.s = nil
}

func (v *SliceVec) Get(i int) *Element {
	return v.s[i]
}

func (v *SliceVec) Len() int {
	return len(v.s)
}

func (v *SliceVec) Size() int64 {
	return v.totalSize
}

func (v *SliceVec) PushBack(row interface{}, size int64) error {
	k, err := v.allocateKey(row)
	if err != nil {
		return err
	}

	v.s = append(v.s, &Element{Row: row, Key: k, Size: size})
	v.totalSize += size

	return nil
}

func (v *SliceVec) PushFrontNoKey(row interface{}, size int64) error {
	v.s = append([]*Element{{Row: row}}, v.s...)
	v.totalSize += size

	return nil
}

func (v *SliceVec) Sort() {
	sort.Slice(v.s, func(i, j int) bool {
		return Less(v.Get(i), v.Get(j))
	})
}

func (v *SliceVec) FrontShift() {
	v.totalSize -= v.s[0].Size
	v.s = v.s[1:]
}
