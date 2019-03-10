package jobcoordinator

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/workerpool"
)

// jobCoordinatorState can be either "running" or "jobCompleted"
type jobCoordinatorState string

const (
	runningJob   = jobCoordinatorState("Running")
	jobCompleted = jobCoordinatorState("jobCompleted")
)

// JobCoordinator holds all the state that the master needs to keep
// track of.
type JobCoordinator struct {
	mutex             sync.Mutex
	conditionVariable *sync.Cond

	address          string
	jobConfiguration *types.JobConfiguration
	rpcServer        *mr_rpc.Server
	workerPool       *workerpool.WorkerPool
	state            jobCoordinatorState
}

// StartJobCoordinator creates a new JobCoordinator and starts it
// running an RPC Server and a WorkerPool.
func StartJobCoordinator(
	masterAddress string,
	jobConfiguration *types.JobConfiguration,
) *JobCoordinator {
	master := &JobCoordinator{
		address:          masterAddress,
		jobConfiguration: jobConfiguration,
		rpcServer:        nil,
		workerPool:       workerpool.Start(),
		state:            runningJob,
	}

	master.conditionVariable = sync.NewCond(&master.mutex)
	master.rpcServer = startJobCoordinatorRPCServer(master)

	return master
}

// Address is merely a getter used elsewhere (simply for logging, I
// think).
func (master *JobCoordinator) Address() string {
	return master.address
}

// MarkJobAsCompleted tells the master that the job is complete. The
// JobCoordinator should now shut itself down.
func (master *JobCoordinator) MarkJobAsCompleted() {
	master.mutex.Lock()
	defer master.mutex.Unlock()

	if master.state == jobCompleted {
		// Ignore redundant requests to shutdown.
		return
	}

	// Tell the RPC server and the workerRegistrationManager to both shut
	// themselves down.
	master.rpcServer.Shutdown()
	master.workerPool.Shutdown()

	// Update our state.
	master.state = jobCompleted

	// And last, let waiters know the job is complete.
	master.conditionVariable.Broadcast()
}

// Wait blocks until the job has completed. This happens when all tasks
// have been scheduled and completed, the final output have been
// computed, and all workers have been shut down.
func (master *JobCoordinator) Wait() {
	master.mutex.Lock()
	defer master.mutex.Unlock()

	for {
		if master.state == jobCompleted {
			return
		}

		master.conditionVariable.Wait()
	}
}
