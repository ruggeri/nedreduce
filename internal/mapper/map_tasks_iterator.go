package mapper

import (
	"io"

	"github.com/ruggeri/nedreduce/internal/types"
)

// MapTasksIterator allows you to iterate through the map tasks
// one-by-one.
type MapTasksIterator struct {
	jobConfiguration *types.JobConfiguration
	nextMapTaskIdx   int
}

// NewMapTasksIterator sets up the iterator.
func NewMapTasksIterator(jobConfiguration *types.JobConfiguration) *MapTasksIterator {
	return &MapTasksIterator{
		jobConfiguration: jobConfiguration,
		nextMapTaskIdx:   0,
	}
}

// Next will step through, one-by-one, each map task until there are
// none left.
func (mapTasksIterator *MapTasksIterator) Next() (*MapTask, error) {
	if mapTasksIterator.nextMapTaskIdx == mapTasksIterator.jobConfiguration.NumMappers() {
		return nil, io.EOF
	}

	mapTask := NewMapTask(
		mapTasksIterator.jobConfiguration,
		mapTasksIterator.nextMapTaskIdx,
	)

	mapTasksIterator.nextMapTaskIdx++

	return &mapTask, nil
}
