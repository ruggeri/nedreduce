package worker

import (
	"sync"

	"github.com/ruggeri/nedreduce/internal/util"
)

// RPCLimitKiller is used to kill the worker after a set number of RPC
// calls.
type RPCLimitKiller struct {
	mutex               sync.Mutex
	numRPCsUntilFailure int
}

// NewRPCLimitKiller makes a new RPCLimitKiller which will start failing
// all RPCs after numRPCsUntilFailure RPCs are performed.
func NewRPCLimitKiller(numRPCsUntilFailure int) *RPCLimitKiller {
	return &RPCLimitKiller{
		mutex:               sync.Mutex{},
		numRPCsUntilFailure: numRPCsUntilFailure,
	}
}

// OnWorkerEvent listens for RPCs, each time getting closer to simply
// failing all future RPCs.
func (rpcLimitKiller *RPCLimitKiller) OnWorkerEvent(
	worker *Worker,
	workerEvent Event,
) Action {
	rpcLimitKiller.mutex.Lock()
	defer rpcLimitKiller.mutex.Unlock()

	switch workerEvent {
	case rpcReceived:
		if rpcLimitKiller.numRPCsUntilFailure > 0 {
			rpcLimitKiller.numRPCsUntilFailure--
		}
	default:
		// do nothing
	}

	if rpcLimitKiller.numRPCsUntilFailure == 0 {
		util.Debug(
			"worker @ %v will fail RPC on purpose.\n", worker.rpcAddress,
		)
		return failRPC
	}

	return doNothing
}
