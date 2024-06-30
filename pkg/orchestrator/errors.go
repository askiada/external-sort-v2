package orchestrator

// orchestratorError is used to define constant errors.
type orchestratorError string

// Error implements the error interface.
func (o orchestratorError) Error() string {
	return string(o)
}

// Error constants.
const (
	ErrNilInput             orchestratorError = "input is nil"
	ErrNilOutput            orchestratorError = "output is nil"
	ErrNilChunksChannel     orchestratorError = "chunks channel is nil"
	ErrNilChunkCreator      orchestratorError = "chunk creator is nil"
	ErrorNilChunkSorter     orchestratorError = "chunk sorter is nil"
	ErrorNilChunksMerger    orchestratorError = "chunks merger is nil"
	ErrorNilTracker         orchestratorError = "tracker is nil"
	ErrFailedToCreateChunks orchestratorError = "failed to create chunks"
	ErrFailedToSortChunk    orchestratorError = "failed to sort chunk"
	ErrFailedToMergeChunks  orchestratorError = "failed to merge chunks"
	ErrFailedToWriteOutput  orchestratorError = "failed to write output"
	ErrFailedToCloseOutput  orchestratorError = "failed to close output"
	ErrNilChunk             orchestratorError = "chunk is nil"
)
