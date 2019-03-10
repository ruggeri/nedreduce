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
//
// Note that mapping/reducing functions are specified by *name*. That is
// because workers need to be able to start up without knowing the
// mapping/reducing functions in advance, and it is not possible to send
// functions themselves over the wire from master to reducer. (Or even
// from client to master.)
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
