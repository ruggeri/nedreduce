package jobcoordinator

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// jobCoordinatorRPCTarget is a dummy type that exposes only those
// methods of the JobCoordinator that should be called via RPC.
type jobCoordinatorRPCTarget struct {
	jobCoordinator *JobCoordinator
}

// RegisterWorker is called by workers after they have started up so
// they can report that they are ready to receive tasks.
func (rpcServerTarget *jobCoordinatorRPCTarget) RegisterWorker(
	WorkerRPCAdress string,
	_ *struct{},
) error {
	util.Debug(
		"jobCoordinator running at %s received RegisterWorker RPC from worker @ %s\n",
		rpcServerTarget.jobCoordinator.address,
		WorkerRPCAdress,
	)

	// The jobCoordinator's workerRegistrationManager is responsible for
	// notifying folks about this new worker.
	rpcServerTarget.jobCoordinator.workerPool.RegisterNewWorker(
		WorkerRPCAdress,
	)

	return nil
}

func (rpcServerTarget *jobCoordinatorRPCTarget) StartJob(
	jobConfiguration *types.JobConfiguration,
	_ *struct{},
) error {
	util.Debug(
		"jobCoordinator running at %s received job submission: %v\n",
		rpcServerTarget.jobCoordinator.address,
		jobConfiguration.JobName,
	)

	return rpcServerTarget.jobCoordinator.StartJob(jobConfiguration)
}

func (rpcServerTarget *jobCoordinatorRPCTarget) WaitForJobCompletion(
	jobName string,
	_ *struct{},
) error {
	return rpcServerTarget.jobCoordinator.WaitForJobCompletion(jobName)
}

// Shutdown is called to shut down the jobCoordinator.
//
// TODO(HIGH): I should re-enable remote shutdown of the jobCoordinator in case
// we want to kill it early.
//
// func (rpcServerTarget *jobCoordinatorRPCTarget) Shutdown(_, _ *struct{})
// error {
//  util.Debug(
//    "jobCoordinator running at %s received Shutdown RPC\n",
//    rpcServerTarget.jobCoordinator.address,
//  )
//
// 	rpcServerTarget.jobCoordinator.Shutdown()
//
// 	return nil
// }

// startJobCoordinatorRPCServer is used by the JobCoordinator to start
// its RPC server.
func startJobCoordinatorRPCServer(jobCoordinator *JobCoordinator) *mr_rpc.Server {
	// Notice how I specify the target's name as "JobCoordinator", even
	// though in theory it would be jobCoordinatorRPCTarget? This is how I
	// obscure those methods of JobCoordinator that I don't want to be
	// RPCable.
	return mr_rpc.StartServer(
		jobCoordinator.address,
		"JobCoordinator",
		&jobCoordinatorRPCTarget{jobCoordinator: jobCoordinator},
	)
}
