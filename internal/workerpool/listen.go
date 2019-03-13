package workerpool

import (
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
		workerPool.handleTaskCompletion(
			message.Address, message.TaskIdentifier,
		)
	case workerRegistrationMessage:
		workerPool.handleWorkerRegistration(message.Address)
	case workerFailedTaskMessage:
		workerPool.handleWorkerFailure(
			message.Address, message.TaskIdentifier, message.Err,
		)
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

	workerPool.currentWorkSet.isStarted = true

	for workerRPCAddress := range workerPool.workerRPCAddresses {
		workerPool.assignTaskToWorker(workerRPCAddress)
	}
}

// handleTaskCompletion handles notification by a worker that a task has
// been completed.
func (workerPool *WorkerPool) handleTaskCompletion(
	workerRPCAddress string,
	taskIdentifier string,
) {
	util.Debug("worker running at %v finished work assignment\n", workerRPCAddress)
	workerPool.currentWorkSet.handleTaskCompletion(taskIdentifier)

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

// handleWorkerFailure handles notification by a worker that it has
// failed.
func (workerPool *WorkerPool) handleWorkerFailure(
	workerRPCAddress string,
	taskIdentifier string,
	err error,
) {
	util.Debug(
		"worker running at %v encountered error %v\n",
		workerRPCAddress,
		err,
	)
	workerPool.currentWorkSet.handleTaskFailure(taskIdentifier)

	// TODO: should try to assign the task to someone else.
}

// handleWorkerRegistration handles registration by a worker.
func (workerPool *WorkerPool) handleWorkerRegistration(newWorkerRPCAddress string) {
	util.Debug("worker running at %v entered WorkerPool\n", newWorkerRPCAddress)

	// Record it in the list of workers.
	if _, ok := workerPool.workerRPCAddresses[newWorkerRPCAddress]; ok {
		// Worker is trying to re-register. Ignore.
		return
	}
	workerPool.workerRPCAddresses[newWorkerRPCAddress] = true

	// And try to assign it work, if there is a currently running workSet.
	if workerPool.currentWorkSet != nil {
		workerPool.assignTaskToWorker(newWorkerRPCAddress)
	}
}

// assignTaskToWorker does like it says. If the work set has no more
// work, then return false.
func (workerPool *WorkerPool) assignTaskToWorker(workerRPCAddress string) {
	if !workerPool.currentWorkSet.isStarted {
		return
	}

	nextTask := workerPool.currentWorkSet.getNextTask()

	if nextTask == nil {
		// We are completed!
		return
	}

	util.Debug("WorkSet: assigning new work to worker running at %v\n", workerRPCAddress)
	nextTask.StartOnWorker(workerRPCAddress, func(err error) {
		if err == nil {
			workerPool.SendWorkerCompletedTaskMessage(
				workerRPCAddress,
				nextTask.Identifier(),
			)
		} else {
			workerPool.SendWorkerFailedTaskMessage(
				workerRPCAddress,
				nextTask.Identifier(),
				err,
			)
		}
	})

	return
}
