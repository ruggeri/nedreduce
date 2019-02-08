package rpc

// ShutdownReply is the RPC reply when a worker's Shutdown method is
// invoked. It returns the number of tasks this worker has processed
// since it was started.
type ShutdownReply struct {
	NumTasksProcessed int
}
