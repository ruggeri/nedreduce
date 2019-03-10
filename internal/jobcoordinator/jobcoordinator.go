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

// JobCoordinator holds all the state that the jobCoordinator needs to
// keep track of.
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
	jobCoordinatorAddress string,
	jobConfiguration *types.JobConfiguration,
) *JobCoordinator {
	jobCoordinator := &JobCoordinator{
		address:          jobCoordinatorAddress,
		jobConfiguration: jobConfiguration,
		rpcServer:        nil,
		workerPool:       workerpool.Start(),
		state:            runningJob,
	}

	jobCoordinator.conditionVariable = sync.NewCond(&jobCoordinator.mutex)
	jobCoordinator.rpcServer = startJobCoordinatorRPCServer(jobCoordinator)

	return jobCoordinator
}

// Address is merely a getter used elsewhere (simply for logging, I
// think).
func (jobCoordinator *JobCoordinator) Address() string {
	return jobCoordinator.address
}

// MarkJobAsCompleted tells the jobCoordinator that the job is complete.
// The JobCoordinator should now shut itself down.
func (jobCoordinator *JobCoordinator) MarkJobAsCompleted() {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	if jobCoordinator.state == jobCompleted {
		// Ignore redundant requests to shutdown.
		return
	}

	// Tell the RPC server and the workerRegistrationManager to both shut
	// themselves down.
	jobCoordinator.rpcServer.Shutdown()
	jobCoordinator.workerPool.Shutdown()

	// Update our state.
	jobCoordinator.state = jobCompleted

	// And last, let waiters know the job is complete.
	jobCoordinator.conditionVariable.Broadcast()
}

// Wait blocks until the job has completed. This happens when all tasks
// have been scheduled and completed, the final output have been
// computed, and all workers have been shut down.
func (jobCoordinator *JobCoordinator) Wait() {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	for {
		if jobCoordinator.state == jobCompleted {
			return
		}

		jobCoordinator.conditionVariable.Wait()
	}
}
