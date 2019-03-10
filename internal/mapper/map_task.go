package mapper

import (
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// A MapTask contains all the information needed to performo a single
// map task.
type MapTask struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunctionName string

	mappingFunction types.MappingFunction
}

// NewMapTask makes a MapTask object from a
// JobConfiguration.
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
		mappingFunction:     nil,
	}
}

func (mapTask *MapTask) MappingFunction() types.MappingFunction {
	if mapTask.mappingFunction == nil {
		mapTask.mappingFunction = util.LoadMappingFunctionByName(mapTask.MappingFunctionName)

	}

	return mapTask.mappingFunction
}
