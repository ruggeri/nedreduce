package workerpool

import "github.com/ruggeri/nedreduce/internal/util"

type completedTaskMessage struct {
	WorkerRPCAddress string
	TaskIdentifier   string
}

func newCompletedTaskMessage(
	workerRPCAddress string,
	taskIdentifier string,
) *completedTaskMessage {
	return &completedTaskMessage{
		WorkerRPCAddress: workerRPCAddress,
		TaskIdentifier:   taskIdentifier,
	}
}

// handleTaskCompletion handles notification by a worker that a task has
// been completed.
func (message *completedTaskMessage) Handle(
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
