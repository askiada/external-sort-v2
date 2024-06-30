package chunksorter

// chunkSorterError is used to define constant errors.
type chunkSorterError string

// Error implements the error interface.
func (o chunkSorterError) Error() string {
	return string(o)
}

// Error constants.
const (
	ErrNilChunkWriter chunkSorterError = "chunk writer is nil"
	ErrNilChunkReader chunkSorterError = "chunk reader is nil"
	ErrNilVector      chunkSorterError = "vector is nil"
	ErrNilKey         chunkSorterError = "key is nil"

	ErrNilVectorFn chunkSorterError = "vector function is nil"
	ErrNilKeyFn    chunkSorterError = "key function is nil"
)
