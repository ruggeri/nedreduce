package jobcoordinator

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runSequentialMapPhase runs a map phase on the jobCoordinator, running
// each map task one-at-a-time.
func runSequentialMapPhase(
	jobCoordinator *JobCoordinator,
) {
	for _, mapTask := range mapper.AllMapTasks(jobCoordinator.jobConfiguration) {
		mapper.ExecuteMapping(&mapTask)
	}
}

// runSequentialReducePhase runs a map phase on the jobCoordinator,
// running each reduce task one-at-a-time.
func runSequentialReducePhase(
	jobCoordinator *JobCoordinator,
) {
	for _, reduceTask := range reducer.AllReduceTasks(jobCoordinator.jobConfiguration) {
		reducer.ExecuteReducing(&reduceTask)
	}
}

// StartSequentialJob runs map and reduce tasks sequentially, waiting
// for each task to complete before running the next.
func StartSequentialJob(
	jobConfiguration *types.JobConfiguration,
) *JobCoordinator {
	jobCoordinator := StartJobCoordinator("jobCoordinator", jobConfiguration)

	// Even though work is done sequentially, it is performed in a
	// background goroutine.
	go jobCoordinator.executeJob(
		runSequentialMapPhase,
		runSequentialReducePhase,
	)

	return jobCoordinator
}
