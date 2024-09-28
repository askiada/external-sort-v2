package key

import (
	"strings"

	"github.com/askiada/external-sort-v2/pkg/model"
)

// String define an string key.
type String struct {
	value string
}

func (k *String) Value() interface{} {
	return k.value
}

// AllocateString create a new string key.
func AllocateString(line string) (model.Key, error) {
	return &String{line}, nil
}

// Less compare two string keys.
func (k *String) Less(other model.Key) bool {
	return k.value < other.(*String).value //nolint //forcetypeassert
}

// Equal check tow string keys are equal.
func (k *String) Equal(other model.Key) bool {
	return k.value == other.(*String).value //nolint //forcetypeassert
}

// UpperString define an string key.
type UpperString struct {
	value string
}

func (k *UpperString) Value() interface{} {
	return k.value
}

// AllocateString create a new string key. It trims space and change the string to uppercase.
func AllocateUpperString(line string) (model.Key, error) {
	return &UpperString{strings.TrimSpace(strings.ToUpper(line))}, nil
}

// Less compare two upper string keys.
func (k *UpperString) Less(other model.Key) bool {
	return k.value < other.(*UpperString).value //nolint //forcetypeassert
}

// Equal check tow upper string keys are equal.
func (k *UpperString) Equal(other model.Key) bool {
	return k.value == other.(*UpperString).value //nolint //forcetypeassert
}
