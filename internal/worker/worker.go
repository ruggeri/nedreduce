package worker

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

type workerRunState string

const (
	running  = workerRunState("running")
	shutDown = workerRunState("shutDown")
)

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	mutex                sync.Mutex
	workerIsShutDownCond *sync.Cond

	rpcAddress  string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	parallelism *Parallelism
	runState    workerRunState

	rpcServer *mr_rpc.Server
}

func StartWorker(
	jobCoordinatorRPCAddress string,
	workerRPCAddress string,
	nRPC int,
	parallelism *Parallelism,
) *Worker {
	worker := &Worker{
		rpcAddress:  workerRPCAddress,
		nRPC:        nRPC,
		nTasks:      0,
		concurrent:  0,
		parallelism: parallelism,
		runState:    running,
	}

	worker.workerIsShutDownCond = sync.NewCond(&worker.mutex)

	worker.rpcServer = startWorkerRPCServer(worker)
	mr_rpc.RegisterWorkerWithJobCoordinator(
		jobCoordinatorRPCAddress,
		workerRPCAddress,
	)

	// TODO: need to, whenever RPC is performed, decrement the number of
	// RPCs left until we just shut down the server.

	return worker
}

func RunWorker(
	jobCoordinatorAddress string,
	workerAddress string,
	nRPC int,
	parallelism *Parallelism,
) {
	// TODO(MEDIUM): what is nRPC?
	worker := StartWorker(
		jobCoordinatorAddress,
		workerAddress,
		nRPC,
		parallelism,
	)

	worker.Wait()
}

// Shutdown is called by the JobCoordinator when all work has been
// completed. We should respond with the number of tasks we have
// processed.
func (worker *Worker) Shutdown() {
	if worker.isShutdown() {
		// Ignore redundant requests to shut down.
		return
	}

	util.Debug(
		"worker @ %s is beginning to shut down\n",
		worker.rpcAddress,
	)

	worker.rpcServer.Shutdown()

	worker.updateRunState(shutDown)

	worker.workerIsShutDownCond.Broadcast()
}

func (worker *Worker) Wait() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	for {
		// FML: can't call isShutdown when you have a lock. :-(
		if worker.runState == shutDown {
			return
		}

		worker.workerIsShutDownCond.Wait()
	}
}

func (worker *Worker) isShutdown() bool {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	return worker.runState == shutDown
}

func (worker *Worker) updateRunState(newRunState workerRunState) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	worker.runState = newRunState
}
