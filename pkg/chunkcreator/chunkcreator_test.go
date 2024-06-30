package chunkcreator_test

import (
	"context"
	"sync"
	"testing"

	"github.com/askiada/external-sort-v2/internal/model"
	"github.com/askiada/external-sort-v2/internal/model/mocks"
	"github.com/askiada/external-sort-v2/pkg/chunkcreator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCreate(t *testing.T) {
	ctx := context.Background()

	// Create a mock reader
	mockReader := mocks.NewMockReader(t)
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data1"), nil).Once()
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data2"), nil).Once()
	mockReader.On("Next").Return(false, nil).Once()
	mockReader.On("Err").Return(nil).Once()

	// Create a mock writer
	mockWriter := mocks.NewMockWriter(t)
	mockWriter.On("WriteRow", mock.Anything, []byte("data1")).Return(nil).Once()
	mockWriter.On("WriteRow", mock.Anything, []byte("data2")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()
	// mockWriter.On("Close").Return(nil).Once()

	// Create the ChunkCreator with the mock functions
	cc := chunkcreator.New(10, func(model.Writer) model.Reader { return mocks.NewMockReader(t) }, func() model.Writer { return mockWriter })
	chunkChan := make(chan model.Reader)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(chunkChan)
		// Call the Create function
		err := cc.Create(ctx, mockReader, chunkChan)
		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()

		chunkReader1 := <-chunkChan
		assert.NotNil(t, chunkReader1)

		_, more := <-chunkChan
		assert.False(t, more)
	}()

	wg.Wait()
}

func TestCreate2chunks(t *testing.T) {
	ctx := context.Background()

	// Create a mock reader
	mockReader := mocks.NewMockReader(t)
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data1"), nil).Once()
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data2"), nil).Once()
	mockReader.On("Next").Return(false, nil).Once()
	mockReader.On("Err").Return(nil).Once()

	// Create a mock writer
	mockWriter := mocks.NewMockWriter(t)
	mockWriter.On("WriteRow", mock.Anything, []byte("data1")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()

	mockWriter.On("WriteRow", mock.Anything, []byte("data2")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()

	// Create the ChunkCreator with the mock functions
	cc := chunkcreator.New(1, func(model.Writer) model.Reader { return mocks.NewMockReader(t) }, func() model.Writer { return mockWriter })

	chunkChan := make(chan model.Reader)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(chunkChan)
		// Call the Create function
		err := cc.Create(ctx, mockReader, chunkChan)
		assert.NoError(t, err)
	}()

	// Verify the channel receives the expected readers
	go func() {
		defer wg.Done()
		// Verify the channel receives the expected readers
		chunkReader1 := <-chunkChan
		assert.NotNil(t, chunkReader1)

		chunkReader2 := <-chunkChan
		assert.NotNil(t, chunkReader2)

		_, more := <-chunkChan
		assert.False(t, more)
	}()

	wg.Wait()
}

func TestCreate2chunksV2(t *testing.T) {
	ctx := context.Background()

	// Create a mock reader
	mockReader := mocks.NewMockReader(t)
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data1"), nil).Once()
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data2"), nil).Once()
	mockReader.On("Next").Return(true, nil).Once()
	mockReader.On("Read").Return([]byte("data3"), nil).Once()
	mockReader.On("Next").Return(false, nil).Once()
	mockReader.On("Err").Return(nil).Once()

	// Create a mock writer
	mockWriter := mocks.NewMockWriter(t)
	mockWriter.On("WriteRow", mock.Anything, []byte("data1")).Return(nil).Once()
	mockWriter.On("WriteRow", mock.Anything, []byte("data2")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()

	mockWriter.On("WriteRow", mock.Anything, []byte("data3")).Return(nil).Once()
	mockWriter.On("Close").Return(nil).Once()
	// mockWriter.On("Close").Return(nil).Once()

	// Create the ChunkCreator with the mock functions
	cc := chunkcreator.New(2, func(model.Writer) model.Reader { return mocks.NewMockReader(t) }, func() model.Writer { return mockWriter })

	chunkChan := make(chan model.Reader)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(chunkChan)
		// Call the Create function
		err := cc.Create(ctx, mockReader, chunkChan)
		assert.NoError(t, err)
	}()

	// Verify the channel receives the expected readers
	go func() {
		defer wg.Done()
		// Verify the channel receives the expected readers
		chunkReader1 := <-chunkChan
		assert.NotNil(t, chunkReader1)

		chunkReader2 := <-chunkChan
		assert.NotNil(t, chunkReader2)

		_, more := <-chunkChan
		assert.False(t, more)
	}()

	wg.Wait()
}
