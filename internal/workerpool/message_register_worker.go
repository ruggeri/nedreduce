package workerpool

import "github.com/ruggeri/nedreduce/internal/util"

type registerWorkerMessage struct {
	WorkerRPCAddress string
}

func newRegisterWorkerMessage(
	workerRPCAddress string,
) *registerWorkerMessage {
	return &registerWorkerMessage{
		WorkerRPCAddress: workerRPCAddress,
	}
}

// handleWorkerRegistration handles registration by a worker.
func (message *registerWorkerMessage) Handle(workerPool *WorkerPool) {
	workerRPCAddress := message.WorkerRPCAddress

	util.Debug(
		"worker running at %v entered WorkerPool\n",
		workerRPCAddress,
	)

	if _, ok := workerPool.workerStates[workerRPCAddress]; ok {
		// Worker is trying to re-register. Ignore.
		//
		// TODO: this could be a place to allow recovery of a worker?
		util.Debug("worker %v tried to re-register?\n", workerRPCAddress)
		return
	}

	// Record them as free for a task.
	workerPool.workerStates[workerRPCAddress] = freeForTask

	// Assign this worker a task.
	workerPool.sendOffMessage(newAssignTaskToWorkerMessage(workerRPCAddress))
}
