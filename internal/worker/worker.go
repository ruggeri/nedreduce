package worker

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

type workerRunState string

const (
	availableForNextJob = workerRunState("availableForNextJob")
	runningJob          = workerRunState("runningJob")
	shutDown            = workerRunState("shutDown")
)

// A Worker executes tasks that are assigned by a JobCoordinator.
type Worker struct {
	mutex                sync.Mutex
	workerIsShutDownCond *sync.Cond

	eventListeners    []EventListener
	numTasksProcessed int
	rpcAddress        string
	rpcServer         *mr_rpc.Server
	runState          workerRunState

	// TODO: Junk instance variables.
	numRPCsUntilSuicide int // quit after this many RPCs; protected by mutex
}

// StartWorker starts up a Worker instance. It will begin listening for
// RPCs (e.g., to execute tasks). It will register with a JobCoordinator
// so the coordinator knows we're in business.
func StartWorker(
	jobCoordinatorRPCAddress string,
	workerRPCAddress string,
	numRPCsUntilSuicide int,
	eventListeners []EventListener,
) *Worker {
	worker := &Worker{
		mutex: sync.Mutex{},
		// See below. Can't setup until we have the mutex.
		workerIsShutDownCond: nil,

		numTasksProcessed: 0,
		rpcAddress:        workerRPCAddress,
		// See below. Can't start until we have a Worker.
		rpcServer: nil,
		runState:  availableForNextJob,

		// TODO: Junk instance variables.
		numRPCsUntilSuicide: numRPCsUntilSuicide,
		eventListeners:      eventListeners,
	}

	// Finish initialization of Cond variable.
	worker.workerIsShutDownCond = sync.NewCond(&worker.mutex)

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
	numRPCsUntilSuicide int,
	eventListeners []EventListener,
) {
	worker := StartWorker(
		jobCoordinatorAddress,
		workerAddress,
		numRPCsUntilSuicide,
		eventListeners,
	)

	worker.Wait()
}

// Shutdown can be called by the JobCoordinator to shutdown this worker.
// We should respond with the number of tasks we have processed.
func (worker *Worker) Shutdown() {
	isShutdown := func() bool {
		worker.mutex.Lock()
		defer worker.mutex.Unlock()
		return worker.runState == shutDown
	}()

	if isShutdown {
		// Ignore redundant requests to shut down.
		return
	}

	util.Debug(
		"worker @ %s is beginning to shut down\n",
		worker.rpcAddress,
	)

	// First shut down the RPC server.
	worker.rpcServer.Shutdown()

	// Next, mark ourselves as having been shut down.
	func() {
		worker.mutex.Lock()
		defer worker.mutex.Unlock()

		worker.runState = shutDown
	}()

	// Last, inform anyone waiting for us to shutdown.
	worker.workerIsShutDownCond.Broadcast()
}

// Wait blocks until the Worker is shutdown.
func (worker *Worker) Wait() {
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

		worker.workerIsShutDownCond.Wait()
	}
}
