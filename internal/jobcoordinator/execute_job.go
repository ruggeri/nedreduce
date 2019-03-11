package jobcoordinator

import (
	"fmt"
	"log"

	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// executeJob executes the various phases of a MapReduce job. It will
// run either in sequential or distributed mode (depending on what the
// JobConfiguration asks for).
func (jobCoordinator *JobCoordinator) executeJob(
	jobConfiguration *types.JobConfiguration,
) {
	var runMapPhase, runReducePhase func(*JobCoordinator, *types.JobConfiguration)

	switch jobConfiguration.ExecutionMode {
	case types.Sequential:
		runMapPhase = runSequentialMapPhase
		runReducePhase = runSequentialReducePhase
	case types.Distributed:
		runMapPhase = runDistributedMapPhase
		runReducePhase = runDistributedReducePhase
	default:
		log.Panicf(
			"Unexpected execution mode: %v\n",
			jobConfiguration.ExecutionMode,
		)
	}

	// Try to clean up all the intermediate files even if something goes
	// wrong in one of the phases.
	defer util.CleanupFiles(jobConfiguration)

	fmt.Printf(
		"%s: Starting Nedreduce Job %s\n",
		jobCoordinator.address,
		jobConfiguration.JobName,
	)

	fmt.Printf("%s: Beginning map phase\n", jobCoordinator.address)
	runMapPhase(jobCoordinator, jobConfiguration)
	fmt.Printf("%s: Map phase completed\n", jobCoordinator.address)

	fmt.Printf("%s: Beginning reduce phase\n", jobCoordinator.address)
	runReducePhase(jobCoordinator, jobConfiguration)
	fmt.Printf("%s: Reduce phase completed\n", jobCoordinator.address)

	fmt.Printf("%s: Beginning final merging\n", jobCoordinator.address)
	util.MergeReducerOutputFiles(
		jobConfiguration.JobName,
		jobConfiguration.NumReducers,
	)
	fmt.Printf("%s: Completed final merging\n", jobCoordinator.address)

	fmt.Printf("%s: Map/Reduce task completed\n", jobCoordinator.address)

	// Mark the job as completed so that resources can be cleaned up and
	// anyone waiting can be notified.
	jobCoordinator.markJobAsCompleted()
}
