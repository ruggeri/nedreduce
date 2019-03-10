package master

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util/worker_registration_manager"
)

// Master can be either "running" or "shutdown"
type masterState string

const (
	runningJob   = masterState("Running")
	jobCompleted = masterState("jobCompleted")
)

// Master holds all the state that the master needs to keep track of.
type Master struct {
	mutex             sync.Mutex
	conditionVariable *sync.Cond

	address                   string
	jobConfiguration          *types.JobConfiguration
	rpcServer                 *mr_rpc.Server
	workerRegistrationManager *worker_registration_manager.WorkerRegistrationManager
	state                     masterState
}

func (master *Master) Address() string {
	return master.address
}

// StartMaster creates a new Master and starts it running an RPC Server
// and WorkerRegistrationManager.
func StartMaster(
	masterAddress string,
	jobConfiguration *types.JobConfiguration,
) *Master {
	master := &Master{
		address:                   masterAddress,
		jobConfiguration:          jobConfiguration,
		rpcServer:                 nil,
		workerRegistrationManager: worker_registration_manager.StartManager(),
		state:                     runningJob,
	}

	master.conditionVariable = sync.NewCond(&master.mutex)
	master.rpcServer = startMasterRPCServer(master)

	return master
}

// Shutdown tells the master to shut itself down. That involves killing
// those goroutines responsible for running the RPC Server and for
// managing the WorkerRegistrationManager.
func (master *Master) Shutdown() {
	master.mutex.Lock()
	defer master.mutex.Unlock()

	if master.state == jobCompleted {
		// Ignore redundant requests to shutdown.
		return
	}

	master.rpcServer.Shutdown()
	master.workerRegistrationManager.SendShutdown()

	master.state = jobCompleted
	master.conditionVariable.Broadcast()
}

// Wait blocks until the currently scheduled work has completed. This
// happens when all tasks have been scheduled and completed, the final
// output have been computed, and all workers have been shut down.
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
