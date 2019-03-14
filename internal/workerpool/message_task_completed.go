package workerpool

import "github.com/ruggeri/nedreduce/internal/util"

// A taskCompletedMessage is sent when a worker finishes a task.
type taskCompletedMessage struct {
	WorkerRPCAddress string
	TaskIdentifier   string
}

func newTaskCompletedMessage(
	workerRPCAddress string,
	taskIdentifier string,
) *taskCompletedMessage {
	return &taskCompletedMessage{
		WorkerRPCAddress: workerRPCAddress,
		TaskIdentifier:   taskIdentifier,
	}
}

func (message *taskCompletedMessage) Handle(
	workerPool *WorkerPool,
) {
	workerRPCAddress := message.WorkerRPCAddress
	taskIdentifier := message.TaskIdentifier

	util.Debug("worker running at %v finished assigned task\n", workerRPCAddress)

	// Tell the workSet that the task is completed.
	workerPool.currentWorkSet.handleTaskCompletion(taskIdentifier)

	// Mark the worker as free for new tasks.
	workerPool.workerStates[workerRPCAddress] = freeForTask

	// This may be the last task of the work set!
	if workerPool.currentWorkSet.isCompleted() {
		workerPool.sendOffMessage(newWorkSetCompletedMessage())
	} else {
		// Else, we want to try to give this worker a new task.
		workerPool.sendOffMessage(newAssignTaskToWorkerMessage(workerRPCAddress))
	}
}
