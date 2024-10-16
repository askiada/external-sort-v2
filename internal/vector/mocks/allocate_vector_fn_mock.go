// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	key "github.com/askiada/external-sort-v2/internal/vector/key"
	mock "github.com/stretchr/testify/mock"

	vector "github.com/askiada/external-sort-v2/internal/vector"
)

// MockAllocateVectorFnfunc is an autogenerated mock type for the AllocateVectorFnfunc type
type MockAllocateVectorFnfunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0
func (_m *MockAllocateVectorFnfunc) Execute(_a0 func(interface{}) (key.Key, error)) vector.Vector {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Execute")
	}

	var r0 vector.Vector
	if rf, ok := ret.Get(0).(func(func(interface{}) (key.Key, error)) vector.Vector); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(vector.Vector)
		}
	}

	return r0
}

// NewMockAllocateVectorFnfunc creates a new instance of MockAllocateVectorFnfunc. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockAllocateVectorFnfunc(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockAllocateVectorFnfunc {
	mock := &MockAllocateVectorFnfunc{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
