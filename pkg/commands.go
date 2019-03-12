package pkg

import (
	job_coordinator "github.com/ruggeri/nedreduce/internal/job_coordinator"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/worker"
)

// RunJobCoordinator starts a new JobCoordinator and then blocks and
// waits for the coordinator to be shut down.
func RunJobCoordinator(
	jobCoordinatorAddress string,
) {
	jobCoordinator := job_coordinator.StartJobCoordinator(jobCoordinatorAddress)
	jobCoordinator.WaitForShutdown()
}

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

// ShutdownJobCoordinator performs an RPC which tells the JobCoordinator
// to shut down.
func ShutdownJobCoordinator(
	jobCoordinatorAddress string,
) {
	mr_rpc.ShutdownJobCoordinator(jobCoordinatorAddress)
}

// ShutdownWorker performs an RPC which tells a Worker to shut down.
func ShutdownWorker(
	workerRPCAddress string,
) {
	mr_rpc.ShutdownWorker(workerRPCAddress)
}

// SubmitJob performs an RPC that tells the JobCoordinator to start
// running the specified job.
func SubmitJob(
	jobCoordinatorAddress string,
	jobConfiguration *types.JobConfiguration,
) {
	mr_rpc.SubmitJob(jobCoordinatorAddress, jobConfiguration)
}

// WaitForJobCompletion blocks until the JobCoordinator has completed
// the specified job. Useful because job submission can be async.
func WaitForJobCompletion(
	jobCoordinatorAddress string,
	jobName string,
) {
	mr_rpc.WaitForJobCompletion(jobCoordinatorAddress, jobName)
}
