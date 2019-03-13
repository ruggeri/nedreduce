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
