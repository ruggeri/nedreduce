package mapper

import . "mapreduce/types"

// A Configuration describes the settings for this map task.
type Configuration struct {
	JobName             string
	MapTaskIdx          int
	MapperInputFileName string
	NumReducers         int
	MappingFunction     MappingFunction
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
