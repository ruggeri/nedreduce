package reducer

import (
	"github.com/ruggeri/nedreduce/internal/types"
)

// A ReduceTask describes the settings for this reduce task.
type ReduceTask struct {
	JobName          string
	NumMappers       int
	ReduceTaskIdx    int
	ReducingFunction types.ReducingFunction
}

// ReduceTaskFromJobConfiguration makes a reducer.ReduceTask object from
// a JobConfiguration.
func ReduceTaskFromJobConfiguration(
	jobConfiguration *types.JobConfiguration,
	reduceTaskIdx int,
) ReduceTask {
	return NewReduceTask(
		jobConfiguration.JobName,
		jobConfiguration.NumMappers(),
		reduceTaskIdx,
		jobConfiguration.ReducingFunction,
	)
}

// NewReduceTask makes a reducer.ReduceTask object.
func NewReduceTask(
	jobName string,
	numMappers int,
	reduceTaskIdx int,
	reducingFunction types.ReducingFunction,
) ReduceTask {
	return ReduceTask{
		JobName:          jobName,
		NumMappers:       numMappers,
		ReduceTaskIdx:    reduceTaskIdx,
		ReducingFunction: reducingFunction,
	}
}
