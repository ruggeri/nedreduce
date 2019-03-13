package workerpool

import (
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
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

	for workerRPCAddress, workerState := range workerPool.workerStates {
		if workerState == freeForTask {
			workerPool.assignAnyTaskToWorker(workerRPCAddress)
		}
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

	workerPool.workerStates[workerRPCAddress] = freeForTask

	if !workerPool.currentWorkSet.isCompleted() {
		// If the work set is not yet completed, try to assign more work to
		// the free worker.
		//
		// Note, this may not assign work if all work is presently handed
		// out.
		workerPool.assignAnyTaskToWorker(workerRPCAddress)
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

	// First, mark the worker as failed.
	workerPool.workerStates[workerRPCAddress] = failed

	// Next, try to give the task to someone else.
	task := workerPool.currentWorkSet.getTaskByIdentifier(taskIdentifier)
	for workerRPCAddress, workerState := range workerPool.workerStates {
		if workerState == freeForTask {
			workerPool.assignTaskToWorker(
				workerRPCAddress,
				task,
			)
		}
	}

	// If everyone is busy, that's fine. Someone will take the task later.
	workerPool.currentWorkSet.handleTaskFailure(taskIdentifier)
}

// handleWorkerRegistration handles registration by a worker.
func (workerPool *WorkerPool) handleWorkerRegistration(newWorkerRPCAddress string) {
	util.Debug("worker running at %v entered WorkerPool\n", newWorkerRPCAddress)

	// Record it in the list of workers.
	if _, ok := workerPool.workerStates[newWorkerRPCAddress]; ok {
		// Worker is trying to re-register. Ignore.
		return
	}
	workerPool.workerStates[newWorkerRPCAddress] = freeForTask

	// And try to assign it work, if there is a currently running workSet.
	if workerPool.currentWorkSet != nil {
		workerPool.assignAnyTaskToWorker(newWorkerRPCAddress)
	}
}

// assignAnyTaskToWorker does like it says.
func (workerPool *WorkerPool) assignAnyTaskToWorker(workerRPCAddress string) {
	if !workerPool.currentWorkSet.isStarted {
		return
	}

	nextTask := workerPool.currentWorkSet.getNextTask()

	if nextTask == nil {
		// We are completed!
		return
	}

	workerPool.assignTaskToWorker(workerRPCAddress, nextTask)
}

// assignTaskToWorker does like it says.
func (workerPool *WorkerPool) assignTaskToWorker(
	workerRPCAddress string,
	task mr_rpc.Task,
) {
	util.Debug("WorkSet: assigning new work to worker running at %v\n", workerRPCAddress)
	task.StartOnWorker(workerRPCAddress, func(err error) {
		if err == nil {
			workerPool.SendWorkerCompletedTaskMessage(
				workerRPCAddress,
				task.Identifier(),
			)
		} else {
			workerPool.SendWorkerFailedTaskMessage(
				workerRPCAddress,
				task.Identifier(),
				err,
			)
		}
	})
}
