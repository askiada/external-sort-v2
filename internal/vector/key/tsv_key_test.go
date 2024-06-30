package key_test

import (
	"testing"

	"github.com/askiada/external-sort-v2/internal/vector/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocateTsv(t *testing.T) {
	// Test cases for AllocateTsv function
	tests := []struct {
		row      interface{}
		pos      []int
		expected string
		err      error
	}{
		{
			row:      []string{"1", "John", "Doe"},
			pos:      []int{0},
			expected: "1",
		},
		{
			row:      []string{"1", "John", "Doe"},
			pos:      []int{1},
			expected: "John",
		},
		{
			row: []string{"1", "John", "Doe"},
			pos: []int{3},
			err: assert.AnError,
		},
		{
			row:      []string{"1", "John", "Doe"},
			pos:      []int{0, 1},
			expected: "1##!##John",
		},
		{
			row:      []string{"1", "10", "Doe"},
			pos:      []int{0, 1},
			expected: "1##!##10",
		},
		{
			row: "tuff",
			pos: []int{0, 1},
			err: assert.AnError,
		},
	}

	for _, test := range tests {
		result, err := key.AllocateCsv(test.row, test.pos...)
		if test.err != nil {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		require.Equal(t, test.expected, result.Value())
	}
}
