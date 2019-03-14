package workerpool

import (
	"github.com/ruggeri/nedreduce/internal/util"
)

// An assignTaskToWorkerMessage is sent when a worker becomes available
// for a new task, or when a task was failed, and we want to try to give
// it to someone else (in which case no WorkerRPCAddress is specified).
type assignTaskToWorkerMessage struct {
	WorkerRPCAddress string
}

func newAssignTaskToWorkerMessage(
	workerRPCAddress string,
) *assignTaskToWorkerMessage {
	return &assignTaskToWorkerMessage{
		WorkerRPCAddress: workerRPCAddress,
	}
}

func (message *assignTaskToWorkerMessage) Handle(
	workerPool *WorkerPool,
) {
	if workerPool.currentWorkSet == nil {
		// We may have been asked to assign a task due to worker
		// registration when there is no current work set.
		return
	}

	workerRPCAddress := message.WorkerRPCAddress

	// When a task is failed, we know there is at least one unit of work,
	// and we don't care who we give it to. That is when workerRPCAddress
	// == "".
	if workerRPCAddress == "" {
		workerRPCAddress = workerPool.getFirstFreeWorker()

		if workerRPCAddress == "" {
			// All workers are currently busy.
			return
		}
	}

	// Get a task to assign to the worker.
	task := workerPool.currentWorkSet.takeFirstUnassignedTask()

	if task == nil {
		// There is no presently available work.
		return
	}

	util.Debug(
		"WorkSet: assigning new task to worker running at %v\n",
		workerRPCAddress,
	)

	// Asynchronously launch the task on the worker via RPC.
	task.StartOnWorker(workerRPCAddress, func(err error) {
		// Note: the only reason it is safe to `sendOffMessage` without
		// checking the runState is because we know that we can't change the
		// `runState` (and thus can't close the message queue) until all
		// tasks are acknowledged one way or the other.
		if err == nil {
			// Task was completed successfully!
			workerPool.sendOffMessage(newTaskCompletedMessage(
				workerRPCAddress,
				task.Identifier(),
			))
		} else {
			// There was an error!
			workerPool.sendOffMessage(newWorkerFailedMessage(
				workerRPCAddress,
				task.Identifier(),
				err,
			))
		}
	})
}

// getFirstFreeWorker returns a worker that is freeForTask, or "" if
// there is none.
func (workerPool *WorkerPool) getFirstFreeWorker() string {
	for workerRPCAddress, workerState := range workerPool.workerStates {
		if workerState == freeForTask {
			return workerRPCAddress
		}
	}

	return ""
}
