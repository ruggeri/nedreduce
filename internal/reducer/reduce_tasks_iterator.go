package reducer

import "github.com/ruggeri/nedreduce/internal/types"

type ReduceTasksIterator struct {
	jobConfiguration  *types.JobConfiguration
	nextReduceTaskIdx int
}

func NewReduceTasksIterator(jobConfiguration *types.JobConfiguration) *ReduceTasksIterator {
	return &ReduceTasksIterator{
		jobConfiguration:  jobConfiguration,
		nextReduceTaskIdx: 0,
	}
}

func (reduceTasksIterator *ReduceTasksIterator) Next() *ReduceTask {
	if reduceTasksIterator.nextReduceTaskIdx == reduceTasksIterator.jobConfiguration.NumReducers {
		return nil
	}

	reduceTask := ReduceTaskFromJobConfiguration(
		reduceTasksIterator.jobConfiguration,
		reduceTasksIterator.nextReduceTaskIdx,
	)

	reduceTasksIterator.nextReduceTaskIdx++

	return &reduceTask
}
