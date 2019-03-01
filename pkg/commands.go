package pkg

import (
	"github.com/ruggeri/nedreduce/internal/master"
	"github.com/ruggeri/nedreduce/internal/worker"
)

// RunSequentialJob runs map and reduce tasks sequentially, waiting for
// each task to complete before running the next.
func RunSequentialJob(
	jobConfiguration *JobConfiguration,
) {
	master := master.RunSequentialJob(jobConfiguration)
	master.Wait()
}

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func RunDistributedJob(
	jobConfiguration *JobConfiguration,
	masterAddress string,
) {
	master := master.RunDistributedJob(jobConfiguration, masterAddress)
	master.Wait()
}

func RunWorker(
	masterAddress string,
	workerAddress string,
	mappingFunction MappingFunction,
	reducingFunction ReducingFunction,
	nRPC int,
) {
	worker.RunWorker(
		masterAddress,
		workerAddress,
		mappingFunction,
		reducingFunction,
		nRPC,
		nil,
	)
}
