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

// NewReduceTask makes a reducer.ReduceTask object from a
// JobConfiguration.
func NewReduceTask(
	jobConfiguration *types.JobConfiguration,
	reduceTaskIdx int,
) ReduceTask {
	return ReduceTask{
		JobName:              jobConfiguration.JobName,
		NumMappers:           jobConfiguration.NumMappers(),
		ReduceTaskIdx:        reduceTaskIdx,
		ReducingFunctionName: jobConfiguration.ReducingFunctionName,
	}
}

func AllReduceTasks(
	jobConfiguration *types.JobConfiguration,
) []ReduceTask {
	numReducers := jobConfiguration.NumReducers
	reduceTasks := []ReduceTask(nil)

	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		reduceTask := NewReduceTask(jobConfiguration, reduceTaskIdx)
		reduceTasks = append(reduceTasks, reduceTask)
	}

	return reduceTasks
}

// ReducingFunction loads the specified reducing function by name.
func (reduceTask *ReduceTask) ReducingFunction() types.ReducingFunction {
	return util.LoadReducingFunctionByName(reduceTask.ReducingFunctionName)
}
