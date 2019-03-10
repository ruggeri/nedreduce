package jobcoordinator

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/workerpool"
)

// Master can be either "running" or "jobCompleted"
type masterState string

const (
	runningJob   = masterState("Running")
	jobCompleted = masterState("jobCompleted")
)

// Master holds all the state that the master needs to keep track of.
//
// TODO(HIGH): rename to JobCoordinator.
type Master struct {
	mutex             sync.Mutex
	conditionVariable *sync.Cond

	address          string
	jobConfiguration *types.JobConfiguration
	rpcServer        *mr_rpc.Server
	workerPool       *workerpool.WorkerPool
	state            masterState
}

// StartMaster creates a new Master and starts it running an RPC Server
// and a WorkerPool.
func StartMaster(
	masterAddress string,
	jobConfiguration *types.JobConfiguration,
) *Master {
	master := &Master{
		address:          masterAddress,
		jobConfiguration: jobConfiguration,
		rpcServer:        nil,
		workerPool:       workerpool.Start(),
		state:            runningJob,
	}

	master.conditionVariable = sync.NewCond(&master.mutex)
	master.rpcServer = startMasterRPCServer(master)

	return master
}

// Address is merely a getter used elsewhere (simply for logging, I
// think).
func (master *Master) Address() string {
	return master.address
}

// MarkJobAsCompleted tells the master that the job is complete. The
// Master should now shut itself down.
func (master *Master) MarkJobAsCompleted() {
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
func (master *Master) Wait() {
	master.mutex.Lock()
	defer master.mutex.Unlock()

	for {
		if master.state == jobCompleted {
			return
		}

		master.conditionVariable.Wait()
	}
}
