package mapper

import (
	"github.com/ruggeri/nedreduce/internal/types"
)

// A Configuration describes the settings for this map task.
type Configuration struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunction     types.MappingFunction
}

// ConfigurationFromJobConfiguration makes a mapper.Configuration object
// from a JobConfiguration.
func ConfigurationFromJobConfiguration(
	jobConfiguration *types.JobConfiguration,
	mapTaskIdx int,
) Configuration {
	return NewConfiguration(
		jobConfiguration.JobName,
		mapTaskIdx,
		jobConfiguration.MapperInputFileNames[mapTaskIdx],
		jobConfiguration.NumReducers,
		jobConfiguration.MappingFunction,
	)
}

// NewConfiguration makes a mapper.Configuration object.
func NewConfiguration(
	jobName string,
	mapTaskIdx int,
	mapperInputFileName string,
	numReducers int,
	mappingFunction types.MappingFunction,
) Configuration {
	return Configuration{
		JobName:             jobName,
		MapTaskIdx:          mapTaskIdx,
		MapperInputFileName: mapperInputFileName,
		NumReducers:         numReducers,
		MappingFunction:     mappingFunction,
	}
}
