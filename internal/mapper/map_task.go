package mapper

import (
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// A MapTask contains all the information needed to perform a single map
// task.
type MapTask struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunctionName string
}

// NewMapTask makes a MapTask object from a JobConfiguration.
func NewMapTask(
	jobConfiguration *types.JobConfiguration,
	mapTaskIdx int,
) MapTask {
	return MapTask{
		JobName:             jobConfiguration.JobName,
		MapTaskIdx:          mapTaskIdx,
		MapperInputFileName: jobConfiguration.MapperInputFileNames[mapTaskIdx],
		NumReducers:         jobConfiguration.NumReducers,
		MappingFunctionName: jobConfiguration.MappingFunctionName,
	}
}

// MappingFunction loads the specified mapping function by name.
func (mapTask *MapTask) MappingFunction() types.MappingFunction {
	return util.LoadMappingFunctionByName(mapTask.MappingFunctionName)
}
