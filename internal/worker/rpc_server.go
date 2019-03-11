package worker

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// workerRPCTarget is a dummy type that exposes only those methods of
// the Worker that should be called via RPC.
type workerRPCTarget struct {
	worker *Worker
}

func (workerRPCTarget *workerRPCTarget) ExecuteMapTask(
	mapTask *mr_rpc.MapTask,
	_ *struct{},
) error {
	worker := workerRPCTarget.worker
	return worker.DoTask(func() {
		mapper.ExecuteMapping((*mapper.MapTask)(mapTask))
	})
}

func (workerRPCTarget *workerRPCTarget) ExecuteReduceTask(
	reduceTask *mr_rpc.ReduceTask,
	_ *struct{},
) error {
	worker := workerRPCTarget.worker
	return worker.DoTask(func() {
		reducer.ExecuteReducing((*reducer.ReduceTask)(reduceTask))
	})
}

// Shutdown is called by the JobCoordinator when all work has been
// completed. We respond with the number of tasks the Worker has
// processed.
func (workerRPCTarget *workerRPCTarget) Shutdown(
	_ *struct{},
	numTasksProcessed *int,
) error {
	worker := workerRPCTarget.worker
	worker.Shutdown()

	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	// TODO: Why reset this?
	worker.nRPC = 1

	*numTasksProcessed = worker.nTasks
	return nil
}

// startWorkerRPCServer is used by the Worker to start its RPC server.
func startWorkerRPCServer(worker *Worker) *mr_rpc.Server {
	// Notice how I specify the target's name as "Worker", even though in
	// theory it would be workerRPCTarget? This is how I obscure those
	// methods of Worker that I don't want to be RPCable.
	return mr_rpc.StartServer(
		worker.rpcAddress,
		"Worker",
		&workerRPCTarget{worker: worker},
	)
}
