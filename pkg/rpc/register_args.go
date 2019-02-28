package rpc

// RegisterArgs is the argument passed to the Master when a Worker
// registers with the Master.
type RegisterArgs struct {
	WorkerRPCAdress string
}
