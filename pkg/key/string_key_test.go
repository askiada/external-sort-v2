package key_test

import (
	"testing"

	"github.com/askiada/external-sort-v2/pkg/key"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringKey(t *testing.T) {
	// Test AllocateString
	strKey, err := key.AllocateString("hello")
	require.NoError(t, err)
	assert.Equal(t, "hello", strKey.Value())

	// Test Less
	strKey1, _ := key.AllocateString("abc")
	strKey2, _ := key.AllocateString("def")
	if !strKey1.Less(strKey2) {
		t.Errorf("Less returned false for strKey1 < strKey2")
	}

	// Test Equal
	strKey3, _ := key.AllocateString("abc")
	if !strKey1.Equal(strKey3) {
		t.Errorf("Equal returned false for strKey1 == strKey3")
	}
}

func TestUpperStringKey(t *testing.T) {
	// Test AllocateUpperString
	upperStrKey, err := key.AllocateUpperString("hello")
	require.NoError(t, err)
	assert.Equal(t, "HELLO", upperStrKey.Value())

	// Test Less
	upperStrKey1, _ := key.AllocateUpperString("abc")
	upperStrKey2, _ := key.AllocateUpperString("def")
	if !upperStrKey1.Less(upperStrKey2) {
		t.Errorf("Less returned false for upperStrKey1 < upperStrKey2")
	}

	// Test Equal
	upperStrKey3, _ := key.AllocateUpperString("abc")
	if !upperStrKey1.Equal(upperStrKey3) {
		t.Errorf("Equal returned false for upperStrKey1 == upperStrKey3")
	}
}
