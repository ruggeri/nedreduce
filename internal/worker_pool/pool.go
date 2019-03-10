package worker_pool

import (
	"os"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

type workerPoolRunState string

const (
	running      = workerPoolRunState("running")
	shuttingDown = workerPoolRunState("shuttingDown")
	shutDown     = workerPoolRunState("shutDown")
)

// A WorkerPool manages a bunch of workers. Workers can register
// whenever they want to. The same WorkerPool can be assigned successive
// collection tasks. Each collection must finish entirely before the
// next can be started.
type WorkerPool struct {
	// messageChannel is an internal message channel.
	messageChannel messageChannel
	// noMoreMessagesWaitGroup lets you wait for all pending messages to
	// be cleared out of the messageChannel.
	noMoreMessagesWaitGroup sync.WaitGroup

	// mutex protects changes to mutable state.
	mutex sync.Mutex
	// workerPoolIsFreeForNewWorkSetCond allows you to wait until the
	// WorkerPool is free for a new work set.
	workerPoolIsFreeForNewWorkSetCond *sync.Cond

	// Mutable state
	//
	// currentWorkSet is the currently assigned set of work.
	currentWorkSet *workSet
	// runState records whether the WorkerPool is running, trying to shut
	// down, or shut down.
	runState workerPoolRunState
	// workerRPCAddresses records all connected workers.
	workerRPCAddresses []string
}

// Start starts an empty WorkerPool with no presently assigned task.
func Start() *WorkerPool {
	workerPool := &WorkerPool{
		messageChannel:          make(messageChannel),
		noMoreMessagesWaitGroup: sync.WaitGroup{},

		mutex: sync.Mutex{},
		// the condition variable cannot be set up until after the mutex is
		// constructed.
		workerPoolIsFreeForNewWorkSetCond: nil,

		currentWorkSet:     nil,
		runState:           running,
		workerRPCAddresses: nil,
	}

	workerPool.workerPoolIsFreeForNewWorkSetCond = sync.NewCond(&workerPool.mutex)

	go workerPool.listenForMessages()

	return workerPool
}

// AssignNewWorkSet synchronously waits until the pool is free. It then
// kicks off the work set to be performed. It doesn't wait for the work
// set to complete; it returns a channel so that the user can decide
// whether/when they want to wait.
func (workerPool *WorkerPool) AssignNewWorkSet(
	tasks []mr_rpc.Task,
) (chan WorkSetResult, error) {
	didSetNewWorkSet := workerPool.setWorkSet(tasks)

	// Sometimes we can't assign a new workSet because the WorkerPool is
	// shutdown.
	if !didSetNewWorkSet {
		return nil, os.ErrClosed
	}

	// If we can, send (asynchronously) a message to start working on the
	// new work set.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() {
		workerPool.messageChannel <- message{
			Kind: startNewWorkSetMessage,
		}
	}()

	// Return the workSet's result channel so that the user can be
	// informed when it is completed.
	return workerPool.currentWorkSet.workSetResultChannel, nil
}

// RegisterNewWorker asynchronously notifies the pool manager that a new
// worker has connected.
func (workerPool *WorkerPool) RegisterNewWorker(
	workerRPCAddress string,
) bool {
	// If the worker pool is either shutting down or shut down, don't try
	// to register the worker.
	if !workerPool.isRunning() {
		return false
	}

	// Queue up another message for the background worker.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() {
		workerPool.messageChannel <- message{
			Kind:    workerRegistrationMessage,
			Address: workerRPCAddress,
		}
	}()

	return true
}

// SendWorkerCompletedTaskMessage asynchronously notifies the pool
// manager that a worker has completed their taskk.
func (workerPool *WorkerPool) SendWorkerCompletedTaskMessage(
	workerAddress string,
) bool {
	// If the worker pool is either shutting down or shut down, don't
	// bother marking the task as completed or trying to give the worker
	// more work.
	if !workerPool.isRunning() {
		return false
	}

	// Queue up another message for the background worker.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() {
		workerPool.messageChannel <- message{
			Kind:    workerCompletedTaskMessage,
			Address: workerAddress,
		}
	}()

	return true
}

// Shutdown tells the WorkerPool to stop processing new messages. It
// synchronously waits for all the pending messages to clear out. It
// terminates the background goroutine.
func (workerPool *WorkerPool) Shutdown() {
	// Begin shutting down the worker pool. This will stop it from trying
	// to process new requests.
	workerPool.updateRunState(shuttingDown)

	// Make sure people waiting to add work realize they won't ever be
	// able to.
	workerPool.workerPoolIsFreeForNewWorkSetCond.Broadcast()

	// Wait for all pending messages to clear out.
	workerPool.noMoreMessagesWaitGroup.Wait()

	// Close the channel (which will also clean up the listener
	// goroutine).
	close(workerPool.messageChannel)

	// Finally, mark ourselves as finally all shut down.
	workerPool.updateRunState(shutDown)
}

// isRunning checks to see whether the workerPool has already been told
// to shutdown. Since it examines the WorkerPool's internal state, it
// needs to temporarily lock.
func (workerPool *WorkerPool) isRunning() bool {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	return workerPool.runState == running
}

// updateRunState updates the WorkerPool's runState. It needs a lock to
// coordinate the change to the WorkerPool's internal state.
//
// I bet I could get away with an atomic test-and-set here if I wanted,
// but that seems like it would add complication (but reduce deadlock
// corner-cases to check)...
func (workerPool *WorkerPool) updateRunState(newRunState workerPoolRunState) {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	workerPool.runState = newRunState
}

// setWorkSet will wait until the WorkerPool is free, and then set the
// currentWorkSet.
func (workerPool *WorkerPool) setWorkSet(
	tasks []mr_rpc.Task,
) bool {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	// Wait until the WorkerPool is free and work can be assigned.
	for {
		if workerPool.runState != running {
			// The WorkerPool may shut down, in which case our work cannot
			// be scheduled.
			return false
		} else if workerPool.currentWorkSet == nil {
			// There is no current work; we can assign some!
			break
		}

		workerPool.workerPoolIsFreeForNewWorkSetCond.Wait()
	}

	// Create the new workSet and assign it as current.
	workerPool.currentWorkSet = newWorkSet(tasks)

	return true
}
