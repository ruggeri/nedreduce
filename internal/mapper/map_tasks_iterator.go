package mapper

import "github.com/ruggeri/nedreduce/internal/types"

type MapTasksIterator struct {
	jobConfiguration *types.JobConfiguration
	nextMapTaskIdx   int
}

func (mapTasksIterator *MapTasksIterator) Next() *MapTask {
	if mapTasksIterator.nextMapTaskIdx == mapTasksIterator.jobConfiguration.NumMappers() {
		return nil
	}

	mapTask := NewMapTask(
		mapTasksIterator.jobConfiguration,
		mapTasksIterator.nextMapTaskIdx,
	)

	mapTasksIterator.nextMapTaskIdx++

	return &mapTask
}
