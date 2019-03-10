package worker_pool

import (
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/util"
)

// listenForMessages is run by a background goroutine to handle
// successive incoming messages.
func (workerPool *WorkerPool) listenForMessages() {
	for message := range workerPool.messageChannel {
		workerPool.handleMessage(message)
	}
}

// handleMessage is used to dispatch to the correct message handler.
func (workerPool *WorkerPool) handleMessage(message message) {
	switch message.Kind {
	case startNewWorkSetMessage:
		workerPool.handleStartingNewWorkSet()
	case workerCompletedTaskMessage:
		workerPool.handleTaskCompletion(message.Address)
	case workerRegistrationMessage:
		workerPool.handleWorkerRegistration(message.Address)
	default:
		log.Panicf("Unexpected message type: %v\n", message.Kind)
	}

	// The message is processed, so there is one less in flight.
	workerPool.noMoreMessagesWaitGroup.Done()
}

// handleStartingNewWorkSet starts working on a workSet by assigning
// tasks to all currently registered workers.
func (workerPool *WorkerPool) handleStartingNewWorkSet() {
	util.Debug("WorkerPool is starting new work set\n")

	for _, workerRPCAddress := range workerPool.workerRPCAddresses {
		if !workerPool.assignTaskToWorker(workerRPCAddress) {
			// We may run out of work to hand out before we run out of
			// workers. In that case we can break early.
			break
		}
	}
}

// handleTaskCompletion handles notification by a worker that a task has
// been completed.
func (workerPool *WorkerPool) handleTaskCompletion(workerRPCAddress string) {
	util.Debug("worker running at %v finished work assignment\n", workerRPCAddress)
	workerPool.currentWorkSet.handleTaskCompletion()

	if !workerPool.currentWorkSet.isCompleted() {
		// If the work set is not yet completed, try to assign more work to
		// the free worker.
		workerPool.assignTaskToWorker(workerRPCAddress)
	} else {
		// Else, the work set is entirely completed!
		util.Debug("WorkerPool: work set has been completed\n")
		workerPool.currentWorkSet = nil
		// Let someone else know they can schedule work on the WorkerPool.
		workerPool.workerPoolIsFreeForNewWorkSetCond.Signal()
	}
}

// handleWorkerRegistration handles registration by a worker.
func (workerPool *WorkerPool) handleWorkerRegistration(newWorkerRPCAddress string) {
	util.Debug("worker running at %v entered WorkerPool\n", newWorkerRPCAddress)

	// Record it in the list of workers.
	workerPool.workerRPCAddresses = append(
		workerPool.workerRPCAddresses,
		newWorkerRPCAddress,
	)

	// And try to assign it work, if there is a currently running workSet.
	if workerPool.currentWorkSet != nil {
		workerPool.assignTaskToWorker(newWorkerRPCAddress)
	}
}

// assignTaskToWorker does like it says. If the work set has no more
// work, then return false.
func (workerPool *WorkerPool) assignTaskToWorker(workerRPCAddress string) bool {
	nextTask, err := workerPool.currentWorkSet.getNextTask()

	if err == io.EOF {
		return false
	} else if err != nil {
		log.Panicf("Unexpcted error getting task from workSet: %v\n", err)
	}

	util.Debug("WorkSet: assigning new work to worker running at %v\n", workerRPCAddress)
	nextTask.StartOnWorker(workerRPCAddress, func() {
		workerPool.SendWorkerCompletedTaskMessage(workerRPCAddress)
	})

	return true
}
