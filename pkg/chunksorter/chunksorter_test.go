package chunksorter_test

import (
	"context"
	"testing"

	"github.com/askiada/external-sort-v2/internal/vector"
	vectormocks "github.com/askiada/external-sort-v2/internal/vector/mocks"
	"github.com/askiada/external-sort-v2/pkg/chunksorter"
	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/external-sort-v2/pkg/model/mocks"
	keymocks "github.com/askiada/external-sort-v2/pkg/model/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockWriter struct {
	mock.Mock
}

func (m *MockWriter) Write(row interface{}) error {
	args := m.Called(row)
	return args.Error(0)
}

type MockReader struct {
	mock.Mock
}

func (m *MockReader) Read() (interface{}, error) {
	args := m.Called()
	return args.Get(0), args.Error(1)
}

func TestChunkSorter_Sort(t *testing.T) {
	// Create mock objects
	mockInputReader := mocks.NewMockReader(t)
	mockWriter := mocks.NewMockWriter(t)

	mockInputReader.On("Next").Return(true, nil).Once()
	mockInputReader.On("Read").Return([]byte("data1"), int64(6), nil).Once()
	mockInputReader.On("Next").Return(true, nil).Once()
	mockInputReader.On("Read").Return([]byte("data2"), int64(6), nil).Once()
	mockInputReader.On("Next").Return(false, nil).Once()
	mockInputReader.On("Err").Return(nil).Once()

	mockWriter.On("WriteRow", mock.Anything, []byte("data1")).Return(nil).Once()
	mockWriter.On("WriteRow", mock.Anything, []byte("data2")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()

	mockVector := vectormocks.NewMockVector(t)
	mockVector.On("PushBack", []byte("data1"), int64(6)).Return(nil).Once()
	mockVector.On("PushBack", []byte("data2"), int64(6)).Return(nil).Once()
	mockVector.On("Sort").Return().Once()
	mockVector.On("Len").Return(2).Once()
	mockVector.On("Get", 0).Return(&vector.Element{Row: []byte("data1")}).Once()
	mockVector.On("Get", 1).Return(&vector.Element{Row: []byte("data2")}).Once()
	mockVector.On("Reset").Return().Once()

	// mockKey := keymocks.NewMockKey(t)
	mockAllocateKeyFn := keymocks.NewMockAllocateKeyFn(t)
	// mockAllocateKeyFn.On("Execute", mock.Anything).Return(mockKey, nil)
	mockAllocateVectorFnfunc := vectormocks.NewMockAllocateVectorFnfunc(t)
	mockAllocateVectorFnfunc.On("Execute", mock.Anything).Return(mockVector).Once()

	// Create the ChunkSorter instance
	sorter := chunksorter.New(
		func() (model.Writer, error) { return mockWriter, nil },
		func(model.Writer) (model.Reader, error) { return mocks.NewMockReader(t), nil },
		mockAllocateKeyFn.Execute,
		mockAllocateVectorFnfunc.Execute,
	)

	// Call the Sort method
	_, err := sorter.Sort(context.Background(), mockInputReader)

	// Assert that the Sort method returned no error
	assert.NoError(t, err)
}
