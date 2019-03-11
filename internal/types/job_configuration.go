package types

// A JobConfiguration describes the settings for this job.
type JobConfiguration struct {
	JobName              string
	MapperInputFileNames []string
	NumReducers          int

	MappingFunctionName  string
	ReducingFunctionName string

	ExecutionMode ExecutionMode
}

// NewJobConfiguration creates a JobConfiguration.
//
// Note that mapping/reducing functions are specified by *name*. That is
// because workers need to be able to start up without knowing the
// mapping/reducing functions in advance, and it is not possible to send
// functions themselves over the wire from jobCoordinator to reducer.
// (Or even from client to jobCoordinator.)
func NewJobConfiguration(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunctionName string,
	reducingFunctionName string,
	executionMode ExecutionMode,
) *JobConfiguration {
	return &JobConfiguration{
		JobName:              jobName,
		MapperInputFileNames: mapperInputFileNames,
		NumReducers:          numReducers,
		MappingFunctionName:  mappingFunctionName,
		ReducingFunctionName: reducingFunctionName,
		ExecutionMode:        executionMode,
	}
}

// NumMappers returns the number of mappers (equal to the number of
// mapper input files).
func (jobConfiguration *JobConfiguration) NumMappers() int {
	return len(jobConfiguration.MapperInputFileNames)
}
