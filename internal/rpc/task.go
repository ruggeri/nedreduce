package rpc

// A Task represents a unit of work which can be performed by an RPC
// worker.
type Task interface {
	// StartOnWorker asynchronously sends the task off to the worker,
	// calling the appropriate RPC method. On completion, the callback is
	// invoked so the originator of the task can be notified.
	StartOnWorker(
		workerAddress string,
		rpcCompletionCallback CompletionCallback,
	)

	// Identifier is a unique identifier for this task.
	Identifier() string
}

// CompletionCallback is simply a callback to let a task author know it
// has been completed. An error may be returned if the worker
// encountered a problem.
type CompletionCallback func(err error)
