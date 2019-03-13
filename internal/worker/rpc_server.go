package worker

import (
	"errors"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// workerRPCTarget is a dummy type that exposes only those methods of
// the Worker that should be called via RPC.
type workerRPCTarget struct {
	worker *Worker
}

// ExecuteMapTask does what it says.
func (workerRPCTarget *workerRPCTarget) ExecuteMapTask(
	mapTask *mr_rpc.MapTask,
	_ *struct{},
) error {
	worker := workerRPCTarget.worker

	util.Debug(
		"worker running at %v received mapTask %v\n",
		worker.rpcAddress,
		mapTask.MapTaskIdx,
	)

	for _, eventListener := range worker.eventListeners {
		if eventListener.OnWorkerEvent(worker, rpcReceived) == failRPC {
			util.Debug("Sending back fake timeout")
			return errors.New("FakeTimeoutError")
		}
	}

	err := worker.DoTask(func() {
		mapper.ExecuteMapping((*mapper.MapTask)(mapTask))
	})

	return err
}

// ExecuteReduceTask does what it says.
func (workerRPCTarget *workerRPCTarget) ExecuteReduceTask(
	reduceTask *mr_rpc.ReduceTask,
	_ *struct{},
) error {
	worker := workerRPCTarget.worker

	util.Debug(
		"worker running at %v received reduceTask %v\n",
		worker.rpcAddress,
		reduceTask.ReduceTaskIdx,
	)

	for _, eventListener := range worker.eventListeners {
		if eventListener.OnWorkerEvent(worker, rpcReceived) == failRPC {
			return errors.New("FakeTimeoutError")
		}
	}

	err := worker.DoTask(func() {
		reducer.ExecuteReducing((*reducer.ReduceTask)(reduceTask))
	})

	return err
}

// Shutdown is called by the JobCoordinator when all work has been
// completed. We respond with the number of tasks the Worker has
// processed.
func (workerRPCTarget *workerRPCTarget) Shutdown(
	_ *struct{},
	numTasksProcessed *int,
) error {
	worker := workerRPCTarget.worker

	util.Debug(
		"worker running at %v received shutodwn RPC\n",
		worker.rpcAddress,
	)

	// Perform the actual shutdown of the worker.
	worker.Shutdown()

	// Get the number of tasks processed by Worker over its lifespan.
	func() {
		worker.mutex.Lock()
		defer worker.mutex.Unlock()
		*numTasksProcessed = worker.numTasksProcessed
	}()

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
