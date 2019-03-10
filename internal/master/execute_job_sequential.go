package master

import (
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runSequentialMapPhase runs a map phase on the master, running each
// map task one-at-a-time.
func runSequentialMapPhase(
	master *Master,
) {
	mapTasksIterator := mapper.NewMapTasksIterator(
		master.jobConfiguration,
	)

	for {
		mapTask, err := mapTasksIterator.Next()

		if err == io.EOF {
			return
		} else if err != nil {
			log.Panicf("Unexpected MapTask iteration error: %v\n", err)
		}

		mapper.ExecuteMapping(mapTask)
	}
}

// runSequentialReducePhase runs a map phase on the master, running each
// reduce task one-at-a-time.
func runSequentialReducePhase(
	master *Master,
) {
	reduceTasksIterator := reducer.NewReduceTasksIterator(
		master.jobConfiguration,
	)

	for {
		reduceTask, err := reduceTasksIterator.Next()

		if err == io.EOF {
			return
		} else if err != nil {
			log.Panicf("Unexpected ReduceTask iteration error: %v\n", err)
		}

		reducer.ExecuteReducing(reduceTask)
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
