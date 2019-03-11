package worker

import "github.com/ruggeri/nedreduce/internal/util"

// RPCLimitKiller is used to kill the worker after a set number of RPC
// calls.
type RPCLimitKiller struct {
	numRPCsUntilSuicide int
}

func NewRPCLimitKiller(numRPCsUntilSuicide int) *RPCLimitKiller {
	return &RPCLimitKiller{
		numRPCsUntilSuicide: numRPCsUntilSuicide,
	}
}

// OnWorkerEvent listens for RPCs, each time getting closer to simply
// failing all future RPCs.
func (rpcLimitKiller *RPCLimitKiller) OnWorkerEvent(
	worker *Worker,
	workerEvent WorkerEvent,
) WorkerAction {
	switch workerEvent {
	case rpcReceived:
		if rpcLimitKiller.numRPCsUntilSuicide > 0 {
			rpcLimitKiller.numRPCsUntilSuicide--
		}
	default:
		// do nothing
	}

	if rpcLimitKiller.numRPCsUntilSuicide == 0 {
		util.Debug(
			"worker @ %v will fail RPC on purpose.", worker.rpcAddress,
		)
		return failRPC
	}

	return doNothing
}
