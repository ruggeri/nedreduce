package worker

import "github.com/ruggeri/nedreduce/internal/util"

// RPCLimitKiller is used to kill the worker after a set number of RPC
// calls.
type RPCLimitKiller struct {
	numRPCsUntilFailure int
}

// NewRPCLimitKiller makes a new RPCLimitKiller which will start failing
// all RPCs after numRPCsUntilFailure RPCs are performed.
func NewRPCLimitKiller(numRPCsUntilFailure int) *RPCLimitKiller {
	return &RPCLimitKiller{
		numRPCsUntilFailure: numRPCsUntilFailure,
	}
}

// OnWorkerEvent listens for RPCs, each time getting closer to simply
// failing all future RPCs.
func (rpcLimitKiller *RPCLimitKiller) OnWorkerEvent(
	worker *Worker,
	workerEvent Event,
) Action {
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
			"worker @ %v will fail RPC on purpose.", worker.rpcAddress,
		)
		return failRPC
	}

	return doNothing
}
