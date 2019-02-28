package types

// A JobConfiguration describes the settings for this job.
type JobConfiguration struct {
	JobName              string
	MapperInputFileNames []string
	NumReducers          int

	MappingFunction  MappingFunction
	ReducingFunction ReducingFunction
}

// NewJobConfiguration creates a JobConfiguration.
func NewJobConfiguration(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunction MappingFunction,
	reducingFunction ReducingFunction,
) JobConfiguration {
	return JobConfiguration{
		JobName:              jobName,
		MapperInputFileNames: mapperInputFileNames,
		NumReducers:          numReducers,
		MappingFunction:      mappingFunction,
		ReducingFunction:     reducingFunction,
	}
}

// NumMappers returns the number of mappers (equal to the number of
// mapper input files).
func (jobConfiguration *JobConfiguration) NumMappers() int {
	return len(jobConfiguration.MapperInputFileNames)
}
