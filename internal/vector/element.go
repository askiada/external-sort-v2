package vector

import "github.com/askiada/external-sort-v2/pkg/model"

type Element struct {
	Key  model.Key
	Row  interface{}
	Size int64
}

// Less returns wether v1 is smaller than v2 based on the keys.
func Less(v1, v2 *Element) bool {
	return v1.Key.Less(v2.Key)
}
