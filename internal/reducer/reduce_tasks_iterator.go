package reducer

import (
	"io"

	"github.com/ruggeri/nedreduce/internal/types"
)

// ReduceTasksIterator allows you to iterate through the reduce tasks
// one-by-one.
type ReduceTasksIterator struct {
	jobConfiguration  *types.JobConfiguration
	nextReduceTaskIdx int
}

// NewReduceTasksIterator sets up the iterator.
func NewReduceTasksIterator(jobConfiguration *types.JobConfiguration) *ReduceTasksIterator {
	return &ReduceTasksIterator{
		jobConfiguration:  jobConfiguration,
		nextReduceTaskIdx: 0,
	}
}

// Next will step through, one-by-one, each reduce task until there are
// none left.
func (reduceTasksIterator *ReduceTasksIterator) Next() (*ReduceTask, error) {
	if reduceTasksIterator.nextReduceTaskIdx == reduceTasksIterator.jobConfiguration.NumReducers {
		return nil, io.EOF
	}

	reduceTask := ReduceTaskFromJobConfiguration(
		reduceTasksIterator.jobConfiguration,
		reduceTasksIterator.nextReduceTaskIdx,
	)

	reduceTasksIterator.nextReduceTaskIdx++

	return &reduceTask, nil
}
