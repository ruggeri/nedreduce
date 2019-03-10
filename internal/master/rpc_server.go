package master

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// masterRPCTarget is a dummy type that exposes only those methods of
// the Master that should be called via RPC.
type masterRPCTarget struct {
	master *Master
}

// RegisterWorker is called by workers after they have started up so
// they can report that they are ready to receive tasks.
func (rpcServerTarget *masterRPCTarget) RegisterWorker(
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
	rpcServerTarget.master.workerRegistrationManager.SendNewWorker(
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

// startMasterRPCServer is used by the Master to start its RPC server.
func startMasterRPCServer(master *Master) *mr_rpc.Server {
	// Notice how I specify the target's name as "Master", even though in
	// theory it would be masterRPCTarget? This is how I obscure those
	// methods of Master that I don't want to be RPCable.
	return mr_rpc.StartServer(master.address, "Master", &masterRPCTarget{master: master})
}
