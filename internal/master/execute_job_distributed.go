package master

import (
	"github.com/ruggeri/nedreduce/internal/types"
)

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func RunDistributedJob(
	jobConfiguration *types.JobConfiguration,
	masterAddress string,
) *Master {
	// First construct the Master and start running an RPC server which
	// can listen for connections.
	master := StartMaster(masterAddress, jobConfiguration)

	go master.executeJob(
		runDistributedMapPhase,
		runDistributedReducePhase,
	)

	return master
}

func runDistributedReducePhase(master *Master) {
}
