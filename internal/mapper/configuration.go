package mapper

import (
	. "github.com/ruggeri/nedreduce/pkg/types"
)

// A Configuration describes the settings for this map task.
type Configuration struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunction     MappingFunction
}

// ConfigurationFromJobConfiguration makes a mapper.Configuration object
// from a JobConfiguration.
func ConfigurationFromJobConfiguration(
	jobConfiguration *JobConfiguration,
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
	mappingFunction MappingFunction,
) Configuration {
	return Configuration{
		JobName:             jobName,
		MapTaskIdx:          mapTaskIdx,
		MapperInputFileName: mapperInputFileName,
		NumReducers:         numReducers,
		MappingFunction:     mappingFunction,
	}
}
