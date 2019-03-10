package master

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runSequentialMapPhase runs a map phase on the master, running each
// map task one-at-a-time.
func runSequentialMapPhase(
	master *Master,
) {
	for _, mapTask := range mapper.AllMapTasks(master.jobConfiguration) {
		mapper.ExecuteMapping(&mapTask)
	}
}

// runSequentialReducePhase runs a map phase on the master, running each
// reduce task one-at-a-time.
func runSequentialReducePhase(
	master *Master,
) {
	for _, reduceTask := range reducer.AllReduceTasks(master.jobConfiguration) {
		reducer.ExecuteReducing(&reduceTask)
	}
}

// StartSequentialJob runs map and reduce tasks sequentially, waiting
// for each task to complete before running the next.
func StartSequentialJob(
	jobConfiguration *types.JobConfiguration,
) *Master {
	master := StartMaster("master", jobConfiguration)

	// Even though work is done sequentially, it is performed in a
	// background goroutine.
	go master.executeJob(
		runSequentialMapPhase,
		runSequentialReducePhase,
	)

	return master
}
