// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	vector "github.com/askiada/external-sort-v2/internal/vector"
	mock "github.com/stretchr/testify/mock"
)

// MockVector is an autogenerated mock type for the Vector type
type MockVector struct {
	mock.Mock
}

// FrontShift provides a mock function with given fields:
func (_m *MockVector) FrontShift() {
	_m.Called()
}

// Get provides a mock function with given fields: i
func (_m *MockVector) Get(i int) *vector.Element {
	ret := _m.Called(i)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 *vector.Element
	if rf, ok := ret.Get(0).(func(int) *vector.Element); ok {
		r0 = rf(i)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*vector.Element)
		}
	}

	return r0
}

// Len provides a mock function with given fields:
func (_m *MockVector) Len() int {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Len")
	}

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// PushBack provides a mock function with given fields: row
func (_m *MockVector) PushBack(row interface{}) error {
	ret := _m.Called(row)

	if len(ret) == 0 {
		panic("no return value specified for PushBack")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(row)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PushFrontNoKey provides a mock function with given fields: row
func (_m *MockVector) PushFrontNoKey(row interface{}) error {
	ret := _m.Called(row)

	if len(ret) == 0 {
		panic("no return value specified for PushFrontNoKey")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(row)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Reset provides a mock function with given fields:
func (_m *MockVector) Reset() {
	_m.Called()
}

// Sort provides a mock function with given fields:
func (_m *MockVector) Sort() {
	_m.Called()
}

// NewMockVector creates a new instance of MockVector. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockVector(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockVector {
	mock := &MockVector{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
