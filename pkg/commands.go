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
	master := jobcoordinator.StartSequentialJob(jobConfiguration)
	master.Wait()
}

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func RunDistributedJob(
	jobConfiguration *JobConfiguration,
	masterAddress string,
) {
	master := jobcoordinator.StartDistributedJob(jobConfiguration, masterAddress)
	master.Wait()
}

// RunWorker will run a worker, connecting to the specified master, and
// listening for RPC instructions at the specified worker address.
func RunWorker(
	masterAddress string,
	workerAddress string,
	nRPC int,
) {
	// TODO(MEDIUM): what is nRPC?
	worker.RunWorker(
		masterAddress,
		workerAddress,
		nRPC,
		nil,
	)
}
