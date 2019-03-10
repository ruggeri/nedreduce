package master

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util/work_assigner"
)

func runDistributedMapPhase(master *Master) {
	mapTasksIterator := mapper.NewMapTasksIterator(master.jobConfiguration)

	workAssigner := work_assigner.Start(
		func() mr_rpc.Task {
			return (*mr_rpc.MapTask)(mapTasksIterator.Next())
		},
		master.workerRegistrationManager.NewWorkerRPCAddressStream(),
	)

	workAssigner.Wait()
}

func runDistributedReducePhase(master *Master) {
	reduceTasksIterator := reducer.NewReduceTasksIterator(master.jobConfiguration)

	workAssigner := work_assigner.Start(
		func() mr_rpc.Task {
			return (*mr_rpc.ReduceTask)(reduceTasksIterator.Next())
		},
		master.workerRegistrationManager.NewWorkerRPCAddressStream(),
	)

	workAssigner.Wait()
}

// StartDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func StartDistributedJob(
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
