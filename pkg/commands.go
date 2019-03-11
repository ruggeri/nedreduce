package pkg

import (
	"github.com/ruggeri/nedreduce/internal/jobcoordinator"
	"github.com/ruggeri/nedreduce/internal/worker"
)

// RunSequentialJob runs map and reduce tasks sequentially, waiting for
// each task to complete before running the next.
func RunSequentialJob(
	jobConfiguration *JobConfiguration,
) {
	jobCoordinator := jobcoordinator.StartSequentialJob(jobConfiguration)
	jobCoordinator.Wait()
}

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the jobCoordinator over RPC.
func RunDistributedJob(
	jobConfiguration *JobConfiguration,
	jobCoordinatorAddress string,
) {
	jobCoordinator := jobcoordinator.StartDistributedJob(
		jobConfiguration,
		jobCoordinatorAddress,
	)
	jobCoordinator.Wait()
}

// RunWorker will run a worker, connecting to the specified
// jobCoordinator, and listening for RPC instructions at the specified
// worker address.
func RunWorker(
	jobCoordinatorAddress string,
	workerAddress string,
) {
	// TODO(MEDIUM): what is nRPC?
	worker.RunWorker(
		jobCoordinatorAddress,
		workerAddress,
		nil,
	)
}
