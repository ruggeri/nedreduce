package worker_registration_manager

import (
	"sync"
)

// WorkerRegistrationManager can be either "running" or "shutdown"
type managerState string

const (
	running  = managerState("Running")
	shutdown = managerState("Shutdown")
)

// A WorkerRegistrationManager keeps track of those workers who register with
// us.
type WorkerRegistrationManager struct {
	// The WorkerRegistrationManager keeps track of the available
	// workerRPCAddresses.
	workerRPCAddresses []string
	// The state is "running" if new worker registrations are being
	// accepted. Else it is "shutdown."
	state managerState

	// Internal threads can mutate the manager's internal state, while
	// external threads may want to query the manager's internal state.
	// Therefore we must synchronize mutation to the manager's internal
	// state.
	mutex sync.Mutex
	// External threads can wait on an update to the WorkerRegistrationManager's
	// internal state. This condition variable is used to signal them.
	conditionVariable *sync.Cond
}

// StartManager creates a new WorkerRegistrationManager and starts a listener
// running on it.
func StartManager() *WorkerRegistrationManager {
	manager := &WorkerRegistrationManager{
		state:              running,
		workerRPCAddresses: []string{},
	}

	manager.conditionVariable = sync.NewCond(&manager.mutex)

	return manager
}

// SendNewWorker notifies the manager of a new worker that has
// connected.
func (manager *WorkerRegistrationManager) SendNewWorker(workerRPCAddress string) {
	// Do everything in a goroutine so that this function won't block the
	// caller.
	go func() {
		// Acquire mutex because we will update the state.
		manager.mutex.Lock()
		defer manager.mutex.Unlock()

		// If manager is shutdown then don't bother.
		if manager.state == shutdown {
			return
		}

		// TODO(MEDIUM): Should probably look for accidental reconnection
		// (deduplicate).
		manager.workerRPCAddresses = append(manager.workerRPCAddresses, workerRPCAddress)

		// Notify and wake any listeners.
		manager.conditionVariable.Broadcast()
	}()
}

// SendShutdown tells a manager that there will be no more connections.
// The internal message listener thread can be told to shut down now.
func (manager *WorkerRegistrationManager) SendShutdown() {
	// Do everything in a goroutine so that this function won't block the
	// caller.
	go func() {
		// Acquire mutex because we will update the state.
		manager.mutex.Lock()
		defer manager.mutex.Unlock()

		// If manager is shutdown then don't bother.
		if manager.state == shutdown {
			return
		}

		manager.state = shutdown

		// Notify and wake any listeners.
		manager.conditionVariable.Broadcast()
	}()
}

// WorkerRPCAddresses gives the caller a list of all workers addresses
// that have ever connected.
func (manager *WorkerRegistrationManager) WorkerRPCAddresses() []string {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return append([]string(nil), manager.workerRPCAddresses...)
}

// NewWorkerRPCAddressStream gives the caller a channel on which
// connecting workers' RPC addresses are piped down.
func (manager *WorkerRegistrationManager) NewWorkerRPCAddressStream() chan string {
	// Check out the workerAddressStreamer to see how the RPC addresses
	// are pushed on down.
	return makeWorkerRPCAddressStream(manager)
}
