package rpc

// WorkerRegistrationMessage is the argument passed to the Master when a
// Worker registers with the Master.
type WorkerRegistrationMessage struct {
	WorkerRPCAdress string
}
