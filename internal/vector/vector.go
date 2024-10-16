package vector

import (
	"github.com/askiada/external-sort-v2/internal/vector/key"
)

// Allocate define a vector and methods to read and write it.
type Allocate struct {
	Vector func(func(row interface{}) (key.Key, error)) Vector
	//FnReader reader.Config
	//FnWriter writer.Config
	Key func(elem interface{}) (key.Key, error)
}

// DefaultVector define a helper function to allocate a vector.
func DefaultVector(allocateKey func(elem interface{}) (key.Key, error) /*, fnReader reader.Config, fnWr writer.Config*/) *Allocate {
	return &Allocate{
		//FnReader: fnReader,
		//FnWriter: fnWr,
		Vector: AllocateSlice,
		Key:    allocateKey,
	}
}

// Vector define a basic interface to manipulate a vector.
//
//go:generate mockery --name Vector --structname MockVector --filename vector_mock.go
type Vector interface {
	// Get Access i-th element
	Get(i int) *Element
	// PushBack Add item at the end
	PushBack(row interface{}) error
	// PushFront Add item at the beginning
	PushFrontNoKey(row interface{}) error
	// FrontShift Remove the first element
	FrontShift()
	// Len Length of the Vector
	Len() int
	// Reset Clear the content in the vector
	Reset()
	// Sort sort the vector in ascending order
	Sort()
}

//go:generate mockery --name AllocateVectorFnfunc --structname MockAllocateVectorFnfunc --filename allocate_vector_fn_mock.go
type AllocateVectorFnfunc func(func(row interface{}) (key.Key, error)) Vector
