package pkg

import (
	job_coordinator "github.com/ruggeri/nedreduce/internal/job_coordinator"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/worker"
)

// RunWorker will run a worker, connecting to the specified
// jobCoordinator, and listening for RPC instructions at the specified
// worker address.
func RunWorker(
	jobCoordinatorAddress string,
	workerAddress string,
) {
	worker.RunWorker(
		jobCoordinatorAddress,
		workerAddress,
		nil,
	)
}

func RunJobCoordinator(
	jobCoordinatorAddress string,
) {
	jobCoordinator := job_coordinator.StartJobCoordinator(jobCoordinatorAddress)
	jobCoordinator.WaitForShutdown()
}

func SubmitJob(
	jobCoordinatorAddress string,
	jobConfiguration *types.JobConfiguration,
) {
	mr_rpc.SubmitJob(jobCoordinatorAddress, jobConfiguration)
}

func WaitForJobCompletion(
	jobCoordinatorAddress string,
	jobName string,
) {
	mr_rpc.WaitForJobCompletion(jobCoordinatorAddress, jobName)
}
