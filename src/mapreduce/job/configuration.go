package job

import (
	"mapreduce/common"
	"mapreduce/mapper"
	"mapreduce/reducer"
)

// A Configuration describes the settings for this job.
type Configuration struct {
	JobName              string
	MapperInputFileNames []string
	NumReducers          int

	MappingFunction  mapper.MappingFunction
	ReducingFunction reducer.ReducingFunction
}

// NewConfiguration creates a Configuration.
func NewConfiguration(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunction mapper.MappingFunction,
	reducingFunction reducer.ReducingFunction,
) Configuration {
	return Configuration{
		JobName:              jobName,
		MapperInputFileNames: mapperInputFileNames,
		NumReducers:          numReducers,
		MappingFunction:      mappingFunction,
		ReducingFunction:     reducingFunction,
	}
}

// NumMappers returns the number of mappers (equal to the number of
// mapper input files).
func (configuration *Configuration) NumMappers() int {
	return len(configuration.MapperInputFileNames)
}

// MapperConfiguration creates a mapper.Configuration object for the
// specified mapTaskIdx.
func (configuration *Configuration) MapperConfiguration(mapTaskIdx int) mapper.Configuration {
	return mapper.NewConfiguration(
		configuration.JobName,
		mapTaskIdx,
		configuration.MapperInputFileNames[mapTaskIdx],
		configuration.NumReducers,
		configuration.MappingFunction,
	)
}

// ReducerConfiguration creates a reducer.Configuration object for the
// specified reduceTaskIdx.
func (configuration *Configuration) ReducerConfiguration(reduceTaskIdx int) reducer.Configuration {
	return reducer.NewConfiguration(
		configuration.JobName,
		configuration.NumMappers(),
		reduceTaskIdx,
		configuration.ReducingFunction,
	)
}

// CleanupFiles removes all intermediate files produced by running
// mapreduce.
func (configuration *Configuration) CleanupFiles() {
	jobName := configuration.JobName
	mapperInputFileNames := configuration.MapperInputFileNames
	numReducers := configuration.NumReducers

	// Clean up mapper output files.
	for mapTaskIdx := range mapperInputFileNames {
		for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
			fileName := common.IntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx)
			common.RemoveFile(fileName)
		}
	}

	// Clean up reducer output files.
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		fileName := common.ReducerOutputFileName(jobName, reduceTaskIdx)
		common.RemoveFile(fileName)
	}

	// Clean up final output file.
	common.RemoveFile("mrtmp." + jobName)
}
