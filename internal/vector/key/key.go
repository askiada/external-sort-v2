package key

// Key define the interface to compare keys to sort.
//
//go:generate mockery --name Key --structname MockKey --filename key_mock.go
type Key interface {
	Equal(v2 Key) bool
	// Less returns wether the key is smaller than v2
	Less(v2 Key) bool

	// Value return the value of the key
	Value() interface{}
}

//go:generate mockery --name AllocateKeyFn --structname MockAllocateKeyFn --filename allocate_key_fn_mock.go
type AllocateKeyFn func(row interface{}) (Key, error)
