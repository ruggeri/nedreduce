package rpc

// WorkerRegistrationMessage is the argument passed to the
// JobCoordinator when a Worker registers with the JobCoordinator.
type WorkerRegistrationMessage struct {
	WorkerRPCAdress string
}
