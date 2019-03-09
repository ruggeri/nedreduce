package mapper

import (
	"github.com/ruggeri/nedreduce/internal/types"
)

// A MapTask contains all the information needed to performo a single
// map task.
type MapTask struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunction     types.MappingFunction
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
		MappingFunction:     jobConfiguration.MappingFunction,
	}
}
