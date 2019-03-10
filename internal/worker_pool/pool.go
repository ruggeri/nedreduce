package worker_pool

import (
	"log"
	"sync"
)

// A WorkerPool manages a bunch of workers. Workers can register
// whenever they want to. The same WorkerPool can be assigned successive
// collection tasks. Each collection must finish entirely before the
// next can be started.
type WorkerPool struct {
	// This is an internal message channel.
	messageChannel messageChannel

	// Use these to coordinate changes to mutable state.
	mutex             sync.Mutex
	conditionVariable *sync.Cond

	// Mutable state
	numWorkersWorking     int
	state                 state
	workerRPCAddresses    []string
	workProducingFunction WorkProducingFunction
	workSetResultChannel  chan WorkSetResult
}

// Start starts an empty WorkerPool with no presently assigned task.
func Start() *WorkerPool {
	workAssigner := &WorkerPool{
		messageChannel: make(messageChannel),

		mutex:             sync.Mutex{},
		conditionVariable: nil,

		numWorkersWorking:     0,
		state:                 freeForNewWorkSetToBeAssigned,
		workerRPCAddresses:    nil,
		workProducingFunction: nil,
		workSetResultChannel:  nil,
	}

	workAssigner.conditionVariable = sync.NewCond(&workAssigner.mutex)

	go workAssigner.listenForMessages()

	return workAssigner
}

// AssignNewWorkSet synchronously waits until the pool is free. It then
// kicks off the work set to be performed. It doesn't wait for the work
// set to complete; it returns a channel so that the user can decide
// whether/when they want to wait.
func (workerPool *WorkerPool) AssignNewWorkSet(
	workProducingFunction WorkProducingFunction,
) chan WorkSetResult {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	// Wait until the WorkerPool is free and work can be assigned.
	for {
		if workerPool.state == freeForNewWorkSetToBeAssigned {
			break
		}

		workerPool.conditionVariable.Wait()
	}

	// Setup mutable state for this next work set of tasks.
	workerPool.workProducingFunction = workProducingFunction
	workerPool.workSetResultChannel = make(chan WorkSetResult)
	workerPool.state = workingThroughWorkSetTasks

	// Kick off execution of this work set.
	go func() {
		workerPool.messageChannel <- message{
			Kind: startNewWorkSetMessage,
		}
	}()

	// Return a channel so that the user can be informed when the work set
	// is completed.
	return workerPool.workSetResultChannel
}

// RegisterNewWorker asynchronously notifies the pool manager that a new
// worker has connected.
func (workerPool *WorkerPool) RegisterNewWorker(workerRPCAddress string) {
	// Do everything in a goroutine so that this function won't block the
	// caller.
	go func() {
		workerPool.messageChannel <- message{
			Kind:    workerRegistrationMessage,
			Address: workerRPCAddress,
		}
	}()
}

// SendWorkerCompletedTaskMessage asynchronously notifies the pool manager
// that a worker has completed their taskk.
func (workerPool *WorkerPool) SendWorkerCompletedTaskMessage(workerAddress string) {
	// Do everything in a goroutine so that this function won't block the
	// caller.
	go func() {
		workerPool.messageChannel <- message{
			Kind:    workerCompletedTaskMessage,
			Address: workerAddress,
		}
	}()
}

func (workerPool *WorkerPool) Shutdown() {
	if workerPool.state != freeForNewWorkSetToBeAssigned {
		// TODO(MEDIUM): It's probably okay to shut down a WorkerPool
		// prematurely.
		log.Panic("WorkerPool was shutdown while working")
	}

	// TODO(HIGH): This should probably kill the workers too!
	close(workerPool.messageChannel)
}
