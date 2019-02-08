package reducer

import . "mapreduce/types"

// A Configuration describes the settings for this reduce task.
type Configuration struct {
	JobName          string
	NumMappers       int
	ReduceTaskIdx    int
	ReducingFunction ReducingFunction
}

// NewConfiguration makes a reducer.Configuration object.
func NewConfiguration(
	jobName string,
	NumMappers int,
	reduceTaskIdx int,
	reducingFunction ReducingFunction,
) Configuration {
	return Configuration{
		JobName:          jobName,
		NumMappers:       NumMappers,
		ReduceTaskIdx:    reduceTaskIdx,
		ReducingFunction: reducingFunction,
	}
}
