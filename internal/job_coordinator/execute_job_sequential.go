package job_coordinator

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runSequentialMapPhase runs a map phase on the jobCoordinator, running
// each map task one-at-a-time.
func runSequentialMapPhase(
	jobCoordinator *JobCoordinator,
	jobConfiguration *types.JobConfiguration,
) {
	for _, mapTask := range mapper.AllMapTasks(jobConfiguration) {
		mapper.ExecuteMapping(&mapTask)
	}
}

// runSequentialReducePhase runs a map phase on the jobCoordinator,
// running each reduce task one-at-a-time.
func runSequentialReducePhase(
	jobCoordinator *JobCoordinator,
	jobConfiguration *types.JobConfiguration,
) {
	for _, reduceTask := range reducer.AllReduceTasks(jobConfiguration) {
		reducer.ExecuteReducing(&reduceTask)
	}
}
