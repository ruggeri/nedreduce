package jobcoordinator

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// masterRPCTarget is a dummy type that exposes only those methods of
// the JobCoordinator that should be called via RPC.
type jobCoordinatorRPCTarget struct {
	master *JobCoordinator
}

// RegisterWorker is called by workers after they have started up so
// they can report that they are ready to receive tasks.
func (rpcServerTarget *jobCoordinatorRPCTarget) RegisterWorker(
	registrationMessage *mr_rpc.WorkerRegistrationMessage,
	_ *struct{},
) error {
	util.Debug(
		"master running at %s received RegisterWorker RPC from worker @ %s\n",
		rpcServerTarget.master.address,
		registrationMessage.WorkerRPCAdress,
	)

	// The master's workerRegistrationManager is responsible for notifying
	// folks about this new worker.
	rpcServerTarget.master.workerPool.RegisterNewWorker(
		registrationMessage.WorkerRPCAdress,
	)

	return nil
}

// Shutdown is called to shut down the master.
//
// TODO(HIGH): I should re-enable remote shutdown of the master in case
// we want to kill it early.
//
// func (rpcServerTarget *masterRPCTarget) Shutdown(_, _ *struct{})
// error {
//  util.Debug(
//    "master running at %s received Shutdown RPC\n",
//    rpcServerTarget.master.address,
//  )
//
// 	rpcServerTarget.master.Shutdown()
//
// 	return nil
// }

// startJobCoordinatorRPCServer is used by the JobCoordinator to start
// its RPC server.
func startJobCoordinatorRPCServer(master *JobCoordinator) *mr_rpc.Server {
	// Notice how I specify the target's name as "JobCoordinator", even
	// though in theory it would be masterRPCTarget? This is how I obscure
	// those methods of JobCoordinator that I don't want to be RPCable.
	return mr_rpc.StartServer(
		master.address,
		"JobCoordinator",
		&jobCoordinatorRPCTarget{master: master},
	)
}
