package workerpool

import "github.com/ruggeri/nedreduce/internal/util"

type workerFailedMessage struct {
	WorkerRPCAddress string
	TaskIdentifier   string
	Err              error
}

func newWorkerFailedMessage(
	workerRPCAddress string,
	taskIdentifier string,
	err error,
) *workerFailedMessage {
	return &workerFailedMessage{
		WorkerRPCAddress: workerRPCAddress,
		TaskIdentifier:   taskIdentifier,
		Err:              err,
	}
}

func (message *workerFailedMessage) Handle(
	workerPool *WorkerPool,
) {
	util.Debug(
		"worker running at %v encountered error %v\n",
		message.WorkerRPCAddress,
		message.Err,
	)

	// First, mark the worker as failed.
	workerPool.workerStates[message.WorkerRPCAddress] = failed

	// Return the failed task to the workSet.
	workerPool.currentWorkSet.handleTaskFailure(message.TaskIdentifier)

	// We know there's one more task to perform, and anyone can do it.
	workerPool.sendOffMessage(newAssignTaskToWorkerMessage(""))
}
