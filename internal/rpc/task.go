package rpc

// A Task represents a unit of work to be performed by an RPC worker.
type Task interface {
	StartOnWorker(workerAddress string, rpcCompletionCallback RPCCompletionCallback)
}
