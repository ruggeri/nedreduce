package worker

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

type workerRunState string

const (
	availableForNextTask = workerRunState("availableForNextTask")
	runningATask         = workerRunState("runningATask")
	shutDown             = workerRunState("shutDown")
)

// A Worker executes tasks that are assigned by a JobCoordinator.
type Worker struct {
	mutex        sync.Mutex
	runStateCond *sync.Cond

	eventListeners    []EventListener
	numTasksProcessed int
	rpcAddress        string
	rpcServer         *mr_rpc.Server
	runState          workerRunState
}

// StartWorker starts up a Worker instance. It will begin listening for
// RPCs (e.g., to execute tasks). It will register with a JobCoordinator
// so the coordinator knows we're in business.
func StartWorker(
	jobCoordinatorRPCAddress string,
	workerRPCAddress string,
	eventListeners []EventListener,
) *Worker {
	worker := &Worker{
		mutex: sync.Mutex{},
		// See below. Can't setup until we have the mutex.
		runStateCond: nil,

		eventListeners:    eventListeners,
		numTasksProcessed: 0,
		rpcAddress:        workerRPCAddress,
		// See below. Can't start until we have a Worker.
		rpcServer: nil,
		runState:  availableForNextTask,
	}

	// Finish initialization of Cond variable.
	worker.runStateCond = sync.NewCond(&worker.mutex)

	// Start the RPC server so we can listen for tasks from
	// JobCoordinator.
	worker.rpcServer = startWorkerRPCServer(worker)

	// Oh yeah, let's register ourselves with the JobCoordinator so it
	// knows to give us tasks.
	mr_rpc.RegisterWorkerWithJobCoordinator(
		jobCoordinatorRPCAddress,
		workerRPCAddress,
	)

	return worker
}

// RunWorker simply starts a worker, then waits for it to be shutdown.
func RunWorker(
	jobCoordinatorAddress string,
	workerAddress string,
	eventListeners []EventListener,
) {
	worker := StartWorker(
		jobCoordinatorAddress,
		workerAddress,
		eventListeners,
	)

	worker.WaitForShutdown()
}

// Shutdown can be called by the JobCoordinator to shutdown this worker.
// We should respond with the number of tasks we have processed.
func (worker *Worker) Shutdown() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	for {
		if worker.runState == shutDown {
			// Someone else has shut us down.
			return
		} else if worker.runState == availableForNextTask {
			// No task is running; we can now shut down!
			break
		}

		worker.runStateCond.Wait()
	}

	util.Debug(
		"worker @ %s is beginning to shut down\n",
		worker.rpcAddress,
	)

	// First shut down the RPC server.
	worker.rpcServer.Shutdown()

	// Mark ourself as shut down.
	worker.runState = shutDown

	// Last, inform anyone waiting for us to shut down.
	worker.runStateCond.Broadcast()
}

// WaitForShutdown blocks until the Worker is shut down.
func (worker *Worker) WaitForShutdown() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	for {
		// FML. I was calling a method `isShutdown` that tried to acquire
		// the mutex. I was deadlocking myself.
		//
		// Idea: try not to have any methods that lock call any other
		// methods that lock (duh). In particular: inline simple methods
		// that need locks.
		if worker.runState == shutDown {
			return
		}

		worker.runStateCond.Wait()
	}
}
