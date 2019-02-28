package reducer

import (
	. "github.com/ruggeri/nedreduce/pkg/types"
)

// A Configuration describes the settings for this reduce task.
type Configuration struct {
	JobName          string
	NumMappers       int
	ReduceTaskIdx    int
	ReducingFunction ReducingFunction
}

// ConfigurationFromJobConfiguration makes a reducer.Configuration
// object from a JobConfiguration.
func ConfigurationFromJobConfiguration(
	jobConfiguration *JobConfiguration,
	reduceTaskIdx int,
) Configuration {
	return NewConfiguration(
		jobConfiguration.JobName,
		jobConfiguration.NumMappers(),
		reduceTaskIdx,
		jobConfiguration.ReducingFunction,
	)
}

// NewConfiguration makes a reducer.Configuration object.
func NewConfiguration(
	jobName string,
	numMappers int,
	reduceTaskIdx int,
	reducingFunction ReducingFunction,
) Configuration {
	return Configuration{
		JobName:          jobName,
		NumMappers:       numMappers,
		ReduceTaskIdx:    reduceTaskIdx,
		ReducingFunction: reducingFunction,
	}
}
