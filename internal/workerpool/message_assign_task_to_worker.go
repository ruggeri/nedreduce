package workerpool

import (
	"github.com/ruggeri/nedreduce/internal/util"
)

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
		// We may have been asked to assign a task, but there may be no
		// current work set anyway.
		return
	}

	workerRPCAddress := message.WorkerRPCAddress

	if workerRPCAddress == "" {
		workerRPCAddress = workerPool.getFirstFreeWorker()

		if workerRPCAddress == "" {
			// All workers are currently busy.
			return
		}
	}

	task := workerPool.currentWorkSet.takeFirstUnassignedTask()

	if task == nil {
		// There is no presently available work.
		return
	}

	util.Debug("WorkSet: assigning new task to worker running at %v\n", workerRPCAddress)

	// Async launch the task on the worker.
	task.StartOnWorker(workerRPCAddress, func(err error) {
		// Note: the only reason it is safe to `sendOffMessage` without
		// checking the runState is because we know that we can't change the
		// `runState` (and thus can't close the message queue) until all
		// tasks are acknowledged one way or the other.
		if err == nil {
			workerPool.sendOffMessage(newCompletedTaskMessage(
				workerRPCAddress,
				task.Identifier(),
			))
		} else {
			workerPool.sendOffMessage(newWorkerFailedMessage(
				workerRPCAddress,
				task.Identifier(),
				err,
			))
		}
	})
}

func (workerPool *WorkerPool) getFirstFreeWorker() string {
	for workerRPCAddress, workerState := range workerPool.workerStates {
		if workerState == freeForTask {
			return workerRPCAddress
		}
	}

	return ""
}
