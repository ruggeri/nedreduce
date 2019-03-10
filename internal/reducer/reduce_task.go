package reducer

import (
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// A ReduceTask describes the settings for this reduce task.
type ReduceTask struct {
	JobName              string
	NumMappers           int
	ReduceTaskIdx        int
	ReducingFunctionName string
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
		jobConfiguration.ReducingFunctionName,
	)
}

// NewReduceTask makes a reducer.ReduceTask object.
func NewReduceTask(
	jobName string,
	numMappers int,
	reduceTaskIdx int,
	reducingFunctionName string,
) ReduceTask {
	return ReduceTask{
		JobName:              jobName,
		NumMappers:           numMappers,
		ReduceTaskIdx:        reduceTaskIdx,
		ReducingFunctionName: reducingFunctionName,
	}
}

// ReducingFunction loads the specified reducing function by name.
func (reduceTask *ReduceTask) ReducingFunction() types.ReducingFunction {
	return util.LoadReducingFunctionByName(reduceTask.ReducingFunctionName)
}
