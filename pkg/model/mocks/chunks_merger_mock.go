// Code generated by mockery v2.46.1. DO NOT EDIT.

package mocks

import (
	context "context"

	model "github.com/askiada/external-sort-v2/pkg/model"
	mock "github.com/stretchr/testify/mock"
)

// MockChunksMerger is an autogenerated mock type for the ChunksMerger type
type MockChunksMerger struct {
	mock.Mock
}

// Merge provides a mock function with given fields: ctx, chunks, outputWriter
func (_m *MockChunksMerger) Merge(ctx context.Context, chunks []model.Reader, outputWriter model.Writer) error {
	ret := _m.Called(ctx, chunks, outputWriter)

	if len(ret) == 0 {
		panic("no return value specified for Merge")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []model.Reader, model.Writer) error); ok {
		r0 = rf(ctx, chunks, outputWriter)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewMockChunksMerger creates a new instance of MockChunksMerger. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockChunksMerger(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockChunksMerger {
	mock := &MockChunksMerger{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
