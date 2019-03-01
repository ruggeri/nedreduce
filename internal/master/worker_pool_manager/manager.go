package worker_pool_manager

import (
	"sync"
)

// WorkerPoolManager can be either "running" or "shutdown"
type managerState string

const (
	running  = managerState("Running")
	shutdown = managerState("Shutdown")
)

// A WorkerPoolManager keeps track of those workers who register with
// us.
type WorkerPoolManager struct {
	// The WorkerPoolManager keeps track of the available
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
	// External threads can wait on an update to the WorkerPoolManager's
	// internal state. This condition variable is used to signal them.
	conditionVariable *sync.Cond
}

// StartManager creates a new WorkerPoolManager and starts a listener
// running on it.
func StartManager() *WorkerPoolManager {
	manager := &WorkerPoolManager{
		state:              running,
		workerRPCAddresses: []string{},
	}

	manager.conditionVariable = sync.NewCond(&manager.mutex)

	return manager
}

// SendNewWorker notifies the manager of a new worker that has
// connected.
func (manager *WorkerPoolManager) SendNewWorker(workerRPCAddress string) {
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

		// TODO: Should probably look for accidental reconnection
		// (deduplicate).
		manager.workerRPCAddresses = append(manager.workerRPCAddresses, workerRPCAddress)

		// Notify and wake any listeners.
		manager.conditionVariable.Broadcast()
	}()
}

// SendShutdown tells a manager that there will be no more connections.
// The internal message listener thread can be told to shut down now.
func (manager *WorkerPoolManager) SendShutdown() {
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

// WorkerRPCAddress gives the caller the present a list of all workers
// addresses that have ever connected.
func (manager *WorkerPoolManager) WorkerRPCAddress() []string {
	// Check out the workerAddressStreamer to see who and how the RPC
	// addresses are pushed on down.
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return append([]string(nil), manager.workerRPCAddresses...)
}

// WorkerRPCAddressStream gives the caller a channel on which
// connecting workers' RPC addresses are piped down.
func (manager *WorkerPoolManager) WorkerRPCAddressStream() chan string {
	// Check out the workerAddressStreamer to see how the RPC addresses
	// are pushed on down.
	return makeWorkerRPCAddressStream(manager)
}
