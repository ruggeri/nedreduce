package pkg

import (
	"github.com/ruggeri/nedreduce/internal/jobcoordinator"
	"github.com/ruggeri/nedreduce/internal/types"
)

// An EmitterFunction is used by a MappingFunction or ReducingFunctino
// to emit KeyValues one at a time.
type EmitterFunction = types.EmitterFunction

type ExecutionMode = types.ExecutionMode

// A GroupIteratorFunction is how a ReducingFunction is one-by-one
// passed the KeyValues that comprise a reduce group.
type GroupIteratorFunction = types.GroupIteratorFunction

// A JobConfiguration describes the settings for this job.
type JobConfiguration = types.JobConfiguration

type JobCoordinator = jobcoordinator.JobCoordinator

// KeyValue is a type used to hold the key/value pairs passed to the map
// and reduce functions.
type KeyValue = types.KeyValue

// A MappingFunction is the type of mapping function supplied by the
// user. It is called once per line of the input file. KeyValues are
// emitted one at a time using the `emitterFunction`.
type MappingFunction = types.MappingFunction

// A ReducingFunction is the type of function the user supplies to do
// the reducing. The ReducingFunction is called once per group. The user
// can emit KeyValues one-by-one using the `emitterFunction`.
type ReducingFunction = types.ReducingFunction

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
	return types.NewJobConfiguration(
		jobName,
		mapperInputFileNames,
		numReducers,
		mappingFunctionName,
		reducingFunctionName,
		executionMode,
	)
}
