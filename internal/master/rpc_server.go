package master

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// masterRPCTarget is a dummy type that exposes only those methods that
// should be called via RPC.
type masterRPCTarget struct {
	master *Master
}

// RegisterWorker is called by workers after they have started up to
// report that they are ready to receive tasks.
func (rpcServerTarget *masterRPCTarget) RegisterWorker(args *mr_rpc.RegisterArgs, _ *struct{}) error {
	util.Debug(
		"master running at %s received RegisterWorker RPC from worker @ %s\n",
		rpcServerTarget.master.Address,
		args.WorkerRPCAdress,
	)

	// The master's workerPoolManager is responsible for notifying folks
	// about this new worker.
	rpcServerTarget.master.workerPoolManager.SendNewWorker(args.WorkerRPCAdress)

	return nil
}

// ShutdownMaster is called to shut down the master.
func (rpcServerTarget *masterRPCTarget) Shutdown(_, _ *struct{}) error {
	util.Debug(
		"master running at %s received Shutdown RPC\n",
		rpcServerTarget.master.Address,
	)

	rpcServerTarget.master.Shutdown()

	return nil
}

func startMasterRPCServer(master *Master) *mr_rpc.Server {
	return mr_rpc.StartServer(master.Address, &masterRPCTarget{master: master})
}
