package key

import (
	"fmt"
	"strconv"
)

// Int define an integer key.
type Int struct {
	value int64
}

func (k *Int) Value() interface{} {
	return k.value
}

// AllocateInt create a new integer key.
func AllocateInt(row interface{}) (Key, error) {
	var (
		num int64
		err error
	)

	switch v := row.(type) {
	case string:
		num, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse int from string %+v: %w", v, err)
		}
	case int:
		num = int64(v)
	case int64:
		num = v
	case int32:
		num = int64(v)
	case int16:
		num = int64(v)
	case int8:
		num = int64(v)
	default:
		return nil, fmt.Errorf("unsupported type for AllocateInt: %T", row)
	}

	return &Int{num}, nil
}

// Less compare two integer keys.
func (k *Int) Less(other Key) bool {
	return k.value < other.(*Int).value //nolint //forcetypeassert
}

// Equal check tow integer keys are equal.
func (k *Int) Equal(other Key) bool {
	return k.value == other.(*Int).value //nolint //forcetypeassert
}

// IntFromSlice define an integer key from a position in a slice of integers.
type IntFromSlice struct {
	value int64
}

func (k *IntFromSlice) Value() interface{} {
	return k.value
}

// AllocateIntFromSlice create a new integer key from a position in a slice of integers.
func AllocateIntFromSlice(row interface{}, intIndex int) (Key, error) {

	if intIndex < 0 {
		return nil, fmt.Errorf("position %d is out of range", intIndex)
	}

	var (
		num int64
		err error
	)

	switch v := row.(type) {

	case []string:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}

		num, err = strconv.ParseInt(v[intIndex], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse int %+v", v[intIndex])
		}
	case []int:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}
		num = int64(v[intIndex])
	case []int64:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}
		num = v[intIndex]
	case []int32:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}
		num = int64(v[intIndex])
	case []int16:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}
		num = int64(v[intIndex])
	case []int8:
		if intIndex >= len(v) {
			return nil, fmt.Errorf("position %d is out of range", intIndex)
		}
		num = int64(v[intIndex])
	default:
		return nil, fmt.Errorf("unsupported type for AllocateIntFromSlice: %T", row)
	}

	return &IntFromSlice{num}, nil
}

// Less compare two integer keys.
func (k *IntFromSlice) Less(other Key) bool {
	return k.value < other.(*IntFromSlice).value //nolint //forcetypeassert
}

// Equal check tow integer keys are equal.
func (k *IntFromSlice) Equal(other Key) bool {
	return k.value == other.(*IntFromSlice).value //nolint //forcetypeassert
}
