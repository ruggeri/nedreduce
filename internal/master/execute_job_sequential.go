package master

import (
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"
)

func runSequentialMapPhase(
	master *Master,
) {
	mapTasksIterator := mapper.NewMapTasksIterator(
		master.jobConfiguration,
	)

	for {
		mapTask, err := mapTasksIterator.Next()

		if err != nil {
			if err == io.EOF {
				return
			}

			log.Fatalf("Unexpected MapTask iteration error: %v\n", err)
		}

		mapper.ExecuteMapping(mapTask)
	}
}

func runSequentialReducePhase(
	master *Master,
) {
	reduceTasksIterator := reducer.NewReduceTasksIterator(
		master.jobConfiguration,
	)

	for {
		reduceTask, err := reduceTasksIterator.Next()

		if err != nil {
			if err == io.EOF {
				return
			}

			log.Fatalf("Unexpected ReduceTask iteration error: %v\n", err)
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

	go master.executeJob(
		runSequentialMapPhase,
		runSequentialReducePhase,
	)

	return master
}
