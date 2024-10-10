package key_test

import (
	"testing"

	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocateInt(t *testing.T) {

	tcs := []struct {
		row interface{}
		err error
	}{
		{row: int8(10)},
		{row: int16(10)},
		{row: int32(10)},
		{row: int64(10)},
		{row: int(10)},
		{row: "10"},
		{row: 10.0, err: assert.AnError},
		{row: "tuff", err: assert.AnError},
	}

	for _, tc := range tcs {
		k, err := key.AllocateInt(tc.row)
		if tc.err != nil {
			assert.Error(t, err)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, int64(10), k.Value())
	}
}

func TestIntLess(t *testing.T) {
	k1, err := key.AllocateInt(10)
	require.NoError(t, err)

	k2, err := key.AllocateInt(20)
	require.NoError(t, err)

	if !k1.Less(k2) {
		t.Error("IntLess returned false for k1 < k2")
	}

	if k2.Less(k1) {
		t.Error("IntLess returned true for k2 < k1")
	}

	if k1.Less(k1) {
		t.Error("IntLess returned true for k1 < k1")
	}
}

func TestIntEqual(t *testing.T) {
	k1, err := key.AllocateInt(10)
	require.NoError(t, err)

	k2, err := key.AllocateInt(20)
	require.NoError(t, err)

	k3, err := key.AllocateInt(10)
	require.NoError(t, err)

	if k1.Equal(k2) {
		t.Error("IntEqual returned true for k1 != k2")
	}

	if !k1.Equal(k3) {
		t.Error("IntEqual returned false for k1 == k3")
	}
}

func TestAllocateIntFromSlice(t *testing.T) {

	tcs := []struct {
		row      interface{}
		intIndex int
		err      error
	}{
		{row: []int8{10, 20, 30}, intIndex: 1},
		{row: []int8{10, 20, 30}, intIndex: 3, err: assert.AnError},
		{row: []int8{10, 20, 30}, intIndex: -1, err: assert.AnError},
		{row: []int8{20, 10, 30}, intIndex: 0},

		{row: []int16{10, 20, 30}, intIndex: 1},
		{row: []int16{10, 20, 30}, intIndex: 3, err: assert.AnError},
		{row: []int16{10, 20, 30}, intIndex: -1, err: assert.AnError},
		{row: []int16{20, 10, 30}, intIndex: 0},

		{row: []int32{10, 20, 30}, intIndex: 1},
		{row: []int32{10, 20, 30}, intIndex: 3, err: assert.AnError},
		{row: []int32{10, 20, 30}, intIndex: -1, err: assert.AnError},
		{row: []int32{20, 10, 30}, intIndex: 0},

		{row: []int64{10, 20, 30}, intIndex: 1},
		{row: []int64{10, 20, 30}, intIndex: 3, err: assert.AnError},
		{row: []int64{10, 20, 30}, intIndex: -1, err: assert.AnError},
		{row: []int64{20, 10, 30}, intIndex: 0},

		{row: []int{10, 20, 30}, intIndex: 1},
		{row: []int{10, 20, 30}, intIndex: 3, err: assert.AnError},
		{row: []int{10, 20, 30}, intIndex: -1, err: assert.AnError},
		{row: []int{20, 10, 30}, intIndex: 0},

		{row: []string{"10", "20", "30"}, intIndex: 1},
		{row: []string{"10", "20", "30"}, intIndex: 3, err: assert.AnError},
		{row: []string{"10", "20", "30"}, intIndex: -1, err: assert.AnError},
		{row: []string{"20", "10", "30"}, intIndex: 0},

		{row: []string{"tuff", "10", "30"}, intIndex: 0, err: assert.AnError},
		{row: "tuff", intIndex: 1, err: assert.AnError},
	}

	for _, tc := range tcs {
		k, err := key.AllocateIntFromSlice(tc.row, tc.intIndex)
		if tc.err != nil {
			assert.Error(t, err)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, int64(20), k.Value())
	}
}

func TestIntFromSliceLess(t *testing.T) {
	k1, err := key.AllocateIntFromSlice([]int{10}, 0)
	require.NoError(t, err)

	k2, err := key.AllocateIntFromSlice([]int{20}, 0)
	require.NoError(t, err)

	if !k1.Less(k2) {
		t.Error("IntFromSliceLess returned false for k1 < k2")
	}

	if k2.Less(k1) {
		t.Error("IntFromSliceLess returned true for k2 < k1")
	}

	if k1.Less(k1) {
		t.Error("IntFromSliceLess returned true for k1 < k1")
	}
}

func TestIntFromSliceEqual(t *testing.T) {
	k1, err := key.AllocateIntFromSlice([]int{10}, 0)
	require.NoError(t, err)

	k2, err := key.AllocateIntFromSlice([]int{20}, 0)
	require.NoError(t, err)

	k3, err := key.AllocateIntFromSlice([]int{10}, 0)
	require.NoError(t, err)

	if k1.Equal(k2) {
		t.Error("IntFromSliceEqual returned true for k1 != k2")
	}

	if !k1.Equal(k3) {
		t.Error("IntFromSliceEqual returned false for k1 == k3")
	}
}
