package mapper

import (
	"io"

	"github.com/ruggeri/nedreduce/internal/types"
)

type MapTasksIterator struct {
	jobConfiguration *types.JobConfiguration
	nextMapTaskIdx   int
}

func NewMapTasksIterator(jobConfiguration *types.JobConfiguration) *MapTasksIterator {
	return &MapTasksIterator{
		jobConfiguration: jobConfiguration,
		nextMapTaskIdx:   0,
	}
}

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
