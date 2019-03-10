package jobcoordinator

import (
	"fmt"

	"github.com/ruggeri/nedreduce/internal/util"
)

// executeJob executes the various phases of a MapReduce job. You can
// specify how to run the map/reduce phases, which lets you pick whether
// the tasks of a phase should be done sequentially or in parallel.
func (jobCoordinator *JobCoordinator) executeJob(
	runMapPhase func(*JobCoordinator),
	runReducePhase func(*JobCoordinator),
) {
	jobConfiguration := jobCoordinator.jobConfiguration

	// Tryu to clean up all the intermediate files even if something goes
	// wrong in one of the phases.
	defer util.CleanupFiles(jobConfiguration)

	fmt.Printf(
		"%s: Starting Map/Reduce task %s\n",
		jobCoordinator.address,
		jobConfiguration.JobName,
	)

	fmt.Printf("%s: Beginning map phase\n", jobCoordinator.address)
	runMapPhase(jobCoordinator)
	fmt.Printf("%s: Map phase completed\n", jobCoordinator.address)

	fmt.Printf("%s: Beginning reduce phase\n", jobCoordinator.address)
	runReducePhase(jobCoordinator)
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
	jobCoordinator.MarkJobAsCompleted()
}
