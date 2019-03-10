package pkg

import "github.com/ruggeri/nedreduce/internal/types"

type EmitterFunction = types.EmitterFunction
type GroupIteratorFunction = types.GroupIteratorFunction
type JobConfiguration = types.JobConfiguration
type KeyValue = types.KeyValue
type MappingFunction = types.MappingFunction
type ReducingFunction = types.ReducingFunction

func NewJobConfiguration(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunctionName string,
	reducingFunctionName string,
) JobConfiguration {
	return types.NewJobConfiguration(
		jobName,
		mapperInputFileNames,
		numReducers,
		mappingFunctionName,
		reducingFunctionName,
	)
}
