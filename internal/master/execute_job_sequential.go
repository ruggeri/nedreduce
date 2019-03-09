package master

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

func runSequentialMapPhase(
	master *Master,
) {
	jobConfiguration := master.jobConfiguration

	// Run each map task one-by-one.
	for mapTaskIdx := 0; mapTaskIdx < jobConfiguration.NumMappers(); mapTaskIdx++ {
		mapperConfiguration := mapper.ConfigurationFromJobConfiguration(jobConfiguration, mapTaskIdx)
		mapper.ExecuteMapping(&mapperConfiguration)
	}
}

func runSequentialReducePhase(
	master *Master,
) {
	jobConfiguration := master.jobConfiguration

	// Run each reduce task one-by-one.
	for reduceTaskIdx := 0; reduceTaskIdx < jobConfiguration.NumReducers; reduceTaskIdx++ {
		reducerConfiguration := reducer.ConfigurationFromJobConfiguration(jobConfiguration, reduceTaskIdx)
		reducer.ExecuteReducing(&reducerConfiguration)
	}
}

// RunSequentialJob runs map and reduce tasks sequentially, waiting for
// each task to complete before running the next.
func RunSequentialJob(
	jobConfiguration *types.JobConfiguration,
) *Master {
	master := StartMaster("master", jobConfiguration)

	go master.executeJob(
		runSequentialMapPhase,
		runSequentialReducePhase,
	)

	return master
}
