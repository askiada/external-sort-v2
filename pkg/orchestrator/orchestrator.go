package orchestrator

import (
	"context"
	"fmt"
	"math"

	"github.com/askiada/external-sort-v2/pkg/model"
	"github.com/askiada/go-pipeline/pkg/pipeline"
)

type Orchestrator struct {
	// ChunkSorter is a sorter for chunks.
	ChunkSorter model.ChunkSorter
	// ChunksMerger is a merger for chunks.
	ChunksMerger model.ChunksMerger
	// ChunkCreator is a creator for chunks.
	ChunkCreator model.ChunkCreator
	// Tracker is a tracker for chunks.
	Tracker model.Tracker

	createChunksSync bool

	logger              model.Logger
	defaultLoggerFields map[string]interface{}
}

func New(
	chunkCreator model.ChunkCreator,
	chunkSorter model.ChunkSorter,
	chunksMerger model.ChunksMerger,
	tracker model.Tracker,
	createChunksSync bool,
) *Orchestrator {
	return &Orchestrator{
		ChunkSorter:      chunkSorter,
		ChunksMerger:     chunksMerger,
		ChunkCreator:     chunkCreator,
		Tracker:          tracker,
		createChunksSync: createChunksSync,
	}
}

func (o *Orchestrator) SetLogger(logger model.Logger) {
	o.logger = logger
}

func (o *Orchestrator) validate() error {
	if o.ChunkCreator == nil {
		return ErrNilChunkCreator
	}

	if o.ChunkSorter == nil {
		return ErrorNilChunkSorter
	}

	if o.ChunksMerger == nil {
		return ErrorNilChunksMerger
	}

	if o.Tracker == nil {
		return ErrorNilTracker
	}

	return nil
}

var (
	prepareChunkLoggerFields = map[string]interface{}{
		"component": "prepare chunk to merge",
	}
)

func estimateTotalChunks(inputContentLenght int64, maxChunkSorter int, totalMaxChunkMemory int64) int {
	for i := maxChunkSorter; i > 0; i-- {
		if inputContentLenght/int64(i) > totalMaxChunkMemory {
			return i
		}
	}

	return 1
}

func (o *Orchestrator) Sort(
	ctx context.Context,
	inputContentLenght int64,
	input model.Reader,
	output model.Writer,
	maxChunkSorter int,
	maxChunkMerger int,
) error {

	if maxChunkSorter < 0 {
		maxChunkSorter = 0
	}

	if maxChunkMerger <= 1 {
		maxChunkMerger = math.MaxInt
	}

	if input == nil || inputContentLenght <= 0 {
		return ErrNilInput
	}

	if output == nil {
		return ErrNilOutput
	}

	err := o.validate()
	if err != nil {
		return fmt.Errorf("failed to validate orchestrator: %w", err)
	}

	pipe, err := pipeline.New(ctx)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}

	o.infof("total chunk memory: %d", o.ChunkCreator.MaxMemory())

	maxChunkSorter = estimateTotalChunks(inputContentLenght, maxChunkSorter, o.ChunkCreator.MaxMemory())

	chunkSize := o.ChunkCreator.MaxMemory() / int64(maxChunkSorter)

	o.infof("total chunks: %d", inputContentLenght/chunkSize)
	o.infof("chunk size: %d bytes", chunkSize)
	o.infof("max concurrent chunk sorters: %d", maxChunkSorter)

	chunksStep, err := pipeline.AddRootStep(pipe, "read input file", func(ctx context.Context, rootChan chan<- model.Reader) error {
		o.debug("reading input file")

		fn := o.ChunkCreator.Create
		if o.createChunksSync {
			fn = o.ChunkCreator.SyncCreate
		}

		err = fn(ctx, input, rootChan, chunkSize)
		if err != nil {
			return fmt.Errorf("%s :%w", err.Error(), ErrFailedToCreateChunks)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add root step: %w", err)
	}

	sortedChunksStep, err := pipeline.AddStepOneToOne(
		pipe,
		"sort chunk",
		chunksStep,
		func(ctx context.Context, chunk model.Reader) (model.Reader, error) {
			o.debug("sorting chunk")
			if chunk == nil {
				return nil, ErrNilChunk
			}

			sortedChunk, err := o.ChunkSorter.Sort(ctx, chunk)
			if err != nil {
				return nil, fmt.Errorf("%s %w", err.Error(), ErrFailedToSortChunk)
			}

			if sortedChunk == nil {
				return nil, ErrNilChunk
			}

			return sortedChunk, nil
		},
		pipeline.StepConcurrency[model.Reader](1),
	)
	if err != nil {
		return fmt.Errorf("failed to add step: %w", err)
	}

	preparedSortedChunksStep, err := pipeline.AddStepFromChan(
		pipe,
		"prepare chunks to merge",
		sortedChunksStep,
		func(ctx context.Context, input <-chan model.Reader, output chan []model.Reader) error {
			var sortedChunks []model.Reader

			o.debug("preparing chunks to merge")
			for {

				select {
				case <-ctx.Done():
					return ctx.Err()
				case chunk, ok := <-input:
					o.withFieldsTrace(prepareChunkLoggerFields, "reading chunk")
					if !ok {
						if len(sortedChunks) > 0 {
							select {
							case <-ctx.Done():
								return ctx.Err()
							default:
								o.withFieldsTrace(prepareChunkLoggerFields, "sending sorted chunks to merge")
								output <- sortedChunks
							}
						}

						return nil
					}

					o.withFieldsTrace(prepareChunkLoggerFields, "adding chunk to merge")
					sortedChunks = append(sortedChunks, chunk)
					/*if len(sortedChunks) == maxChunkMerger {
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							o.withFieldsTrace(prepareChunkLoggerFields, "sending sorted chunks to merge")
							output <- sortedChunks
						}

						o.withFieldsTrace(prepareChunkLoggerFields, "resetting sorted chunks")
						sortedChunks = nil
					}*/
				}
			}
		},
	)
	if err != nil {
		return fmt.Errorf("failed to add step: %w", err)
	}

	err = pipeline.AddSink(pipe, "write output file", preparedSortedChunksStep, func(ctx context.Context, chunks []model.Reader) error {
		o.debug("merging chunks")
		err := o.ChunksMerger.Merge(ctx, chunks, output)
		if err != nil {
			return fmt.Errorf("%s %w", err.Error(), ErrFailedToMergeChunks)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add sink: %w", err)
	}

	err = pipe.Run()
	if err != nil {
		return fmt.Errorf("failed to run pipeline: %w", err)
	}

	return nil
}
