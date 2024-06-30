package chunksmerger_test

/*
import (
	"context"
	"testing"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/model/mocks"
	"github.com/askiada/external-sort-v2/internal/vector"
	keymocks "github.com/askiada/external-sort-v2/internal/vector/key/mocks"
	vectormocks "github.com/askiada/external-sort-v2/internal/vector/mocks"
	"github.com/askiada/external-sort-v2/pkg/chunksmerger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	// Create mock readers
	mockReader1 := mocks.NewMockReader(t)
	mockReader2 := mocks.NewMockReader(t)

	// Create mock writer
	mockWriter := mocks.NewMockWriter(t)

	// Create expected rows
	expectedRows := []interface{}{
		[]byte("Alice"),
		[]byte("Charlie"),
		[]byte("Bob"),
		[]byte("David"),
	}

	// Set up expectations for mock readers
	mockReader1.On("Next").Return(true, nil).Once()
	mockReader1.On("Read").Return(expectedRows[0], nil).Once()
	mockReader1.On("Next").Return(true, nil).Once()
	mockReader1.On("Read").Return(expectedRows[1], nil).Once()
	mockReader1.On("Next").Return(false, nil).Once()
	mockReader1.On("Next").Return(false, nil).Once()
	mockReader1.On("Err").Return(nil)
	mockReader1.On("Len").Return(0)

	mockReader2.On("Next").Return(true, nil).Once()
	mockReader2.On("Read").Return(expectedRows[2], nil).Once()
	mockReader2.On("Next").Return(true, nil).Once()
	mockReader2.On("Read").Return(expectedRows[3], nil).Once()
	mockReader2.On("Next").Return(false, nil).Once()
	mockReader2.On("Next").Return(false, nil).Once()
	mockReader2.On("Err").Return(nil)
	mockReader2.On("Len").Return(0)

	outputreader := mocks.NewMockReader(t)
	// Set up expectations for mock writer
	mockWriter.On("WriteRow", mock.Anything, expectedRows[0]).Return(nil).Once().Run(func(args mock.Arguments) {
		outputreader.On("Next").Return(true).Once()
		outputreader.On("Read").Return(args.Get(1), nil).Once()
	})
	mockWriter.On("WriteRow", mock.Anything, expectedRows[2]).Return(nil).Once().Run(func(args mock.Arguments) {
		outputreader.On("Next").Return(true).Once()
		outputreader.On("Read").Return(args.Get(1), nil).Once()
	})
	mockWriter.On("WriteRow", mock.Anything, expectedRows[1]).Return(nil).Once().Run(func(args mock.Arguments) {
		outputreader.On("Next").Return(true).Once()
		outputreader.On("Read").Return(args.Get(1), nil).Once()
	})
	mockWriter.On("WriteRow", mock.Anything, expectedRows[3]).Return(nil).Once().Run(func(args mock.Arguments) {
		outputreader.On("Next").Return(true).Once()
		outputreader.On("Read").Return(args.Get(1), nil).Once()
	})
	mockWriter.On("Close").Return(nil).Once()

	outputreader.On("Next").Return(false).Once()

	mockAllocateKeyFn := keymocks.NewMockAllocateKeyFn(t)
	mockAllocateVectorFnfunc := vectormocks.NewMockAllocateVectorFnfunc(t)

	outputMockVector := vectormocks.NewMockVector(t)
	outputMockVector.On("Get", 0).Return(&vector.Element{Row: expectedRows[0]}).Once()
	outputMockVector.On("Get", 1).Return(&vector.Element{Row: expectedRows[2]}).Once()
	outputMockVector.On("Get", 2).Return(&vector.Element{Row: expectedRows[1]}).Once()
	outputMockVector.On("Get", 3).Return(&vector.Element{Row: expectedRows[3]}).Once()
	outputMockVector.On("Len").Return(1).Once()
	outputMockVector.On("Len").Return(2).Once()
	outputMockVector.On("Len").Return(3).Once()
	outputMockVector.On("Len").Return(4).Once()

	outputMockVector.On("Reset").Return().Once()

	mockKey := keymocks.NewMockKey(t)
	mockKey.On("Less", mock.Anything).Return(true)

	chunk1MockVector := vectormocks.NewMockVector(t)
	chunk2MockVector := vectormocks.NewMockVector(t)

	chunk1MockVector.On("PushBack", expectedRows[0]).Return(nil).Once()
	chunk1MockVector.On("PushBack", expectedRows[1]).Return(nil).Once()
	chunk2MockVector.On("PushBack", expectedRows[2]).Return(nil).Once()
	chunk2MockVector.On("PushBack", expectedRows[3]).Return(nil).Once()

	//Reset Order
	chunk1MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[0]}).Once()
	chunk2MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[2]}).Once()

	// First iteration
	// get min
	chunk1MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[0]}).Once()
	outputMockVector.On("PushBack", expectedRows[0]).Return(nil).Once()

	// update chunks
	chunk1MockVector.On("FrontShift").Return().Once()
	chunk1MockVector.On("Len").Return(1).Once()

	// moveFirstChunkToCorrectIndex
	chunk1MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[1]}).Once()
	chunk2MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[2]}).Once()

	// Second iteration
	// get min
	chunk2MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[2]}).Once()
	outputMockVector.On("PushBack", expectedRows[2]).Return(nil).Once()

	// update chunks
	chunk2MockVector.On("FrontShift").Return().Once()
	chunk2MockVector.On("Len").Return(1).Once()

	// moveFirstChunkToCorrectIndex
	chunk1MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[1]}).Once()
	chunk2MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[3]}).Once()

	// Third iteration
	// get min
	chunk1MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[1]}).Once()
	outputMockVector.On("PushBack", expectedRows[1]).Return(nil).Once()

	// update chunks
	chunk1MockVector.On("FrontShift").Return().Once()
	chunk1MockVector.On("Len").Return(0).Once()
	chunk1MockVector.On("Len").Return(0).Once()

	// chunk is empty, shrink

	// Fourth iteration
	chunk2MockVector.On("Get", 0).Return(&vector.Element{Key: mockKey, Row: expectedRows[3]}).Once()
	outputMockVector.On("PushBack", expectedRows[3]).Return(nil).Once()

	chunk2MockVector.On("FrontShift").Return().Once()
	chunk2MockVector.On("Len").Return(0).Once()
	chunk2MockVector.On("Len").Return(0).Once()

	outputMockVector.On("Reset").Return().Once()
	outputMockVector.On("Len").Return(0).Once()
	outputMockVector.On("Len").Return(0).Once()
	outputMockVector.On("Reset").Return().Once()

	mockAllocateVectorFnfunc.On("Execute", mock.Anything).Return(outputMockVector).Once()
	mockAllocateVectorFnfunc.On("Execute", mock.Anything).Return(chunk1MockVector).Once()
	mockAllocateVectorFnfunc.On("Execute", mock.Anything).Return(chunk2MockVector).Once()

	// Create ChunksMerger instance
	merger := chunksmerger.New(
		func() model.Writer { return mockWriter },
		func(model.Writer) model.Reader { return outputreader },
		mockAllocateKeyFn.Execute,
		mockAllocateVectorFnfunc.Execute,
		10,
		false,
	)

	// Call Merge function
	rdr, err := merger.Merge(context.Background(), []model.Reader{mockReader1, mockReader2})
	require.NoError(t, err)
	i := 0
	sortedExpectedRows := []interface{}{
		expectedRows[0],
		expectedRows[2],
		expectedRows[1],
		expectedRows[3],
	}
	for rdr.Next() {
		got, err := rdr.Read()
		require.NoError(t, err)
		require.Equal(t, sortedExpectedRows[i], got)
		i++
	}
}
*/
