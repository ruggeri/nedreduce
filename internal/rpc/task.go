package rpc

// A Task represents a unit of work which can be performed by an RPC
// worker.
type Task interface {
	// StartOnWorker asynchronously sends the task off to the worker,
	// calling the appropriate RPC method. On completion, the callback is
	// invoked so the originator of the task can be notified.
	StartOnWorker(workerAddress string, rpcCompletionCallback CompletionCallback)
}

// CompletionCallback is simply a callback to let a task author know
// it has been completed.
type CompletionCallback func()
