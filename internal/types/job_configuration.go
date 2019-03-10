package types

// A JobConfiguration describes the settings for this job.
type JobConfiguration struct {
	JobName              string
	MapperInputFileNames []string
	NumReducers          int

	MappingFunctionName  string
	ReducingFunctionName string
}

// NewJobConfiguration creates a JobConfiguration.
func NewJobConfiguration(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunctionName string,
	reducingFunctionName string,
) JobConfiguration {
	return JobConfiguration{
		JobName:              jobName,
		MapperInputFileNames: mapperInputFileNames,
		NumReducers:          numReducers,
		MappingFunctionName:  mappingFunctionName,
		ReducingFunctionName: reducingFunctionName,
	}
}

// NumMappers returns the number of mappers (equal to the number of
// mapper input files).
func (jobConfiguration *JobConfiguration) NumMappers() int {
	return len(jobConfiguration.MapperInputFileNames)
}
