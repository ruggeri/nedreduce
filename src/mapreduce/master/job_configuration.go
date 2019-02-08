package master

import (
	"mapreduce/mapper"
	"mapreduce/reducer"
)

// A JobConfiguration describes the settings for this job.
type JobConfiguration struct {
	JobName              string
	MapperInputFileNames []string
	NumReducers          int

	MappingFunction  mapper.MappingFunction
	ReducingFunction reducer.ReducingFunction
}

// MapperConfiguration creates a mapper.Configuration object for the
// specified mapTaskIdx.
func (jobConfiguration *JobConfiguration) MapperConfiguration(mapTaskIdx int) mapper.Configuration {
	return mapper.Configuration{
		JobName:             jobConfiguration.JobName,
		MapTaskIdx:          mapTaskIdx,
		MapperInputFileName: jobConfiguration.MapperInputFileNames[mapTaskIdx],
		NumReducers:         jobConfiguration.NumReducers,
		MappingFunction:     jobConfiguration.MappingFunction,
	}
}

// ReducerConfiguration creates a reducer.Configuration object for the
// specified reduceTaskIdx.
func (jobConfiguration *JobConfiguration) ReducerConfiguration(reduceTaskIdx int) reducer.Configuration {
	return reducer.Configuration{
		JobName:          jobConfiguration.JobName,
		NumMappers:       jobConfiguration.NumMappers(),
		ReduceTaskIdx:    reduceTaskIdx,
		ReducingFunction: jobConfiguration.ReducingFunction,
	}
}

// NumMappers returns the number of mappers (equal to the number of
// mapper input files).
func (jobConfiguration *JobConfiguration) NumMappers() int {
	return len(jobConfiguration.MapperInputFileNames)
}
