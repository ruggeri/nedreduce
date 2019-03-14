package workerpool

import "github.com/ruggeri/nedreduce/internal/util"

// A registerWorkerMessage is sent when a new worker wants to register.
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

func (message *registerWorkerMessage) Handle(workerPool *WorkerPool) {
	workerRPCAddress := message.WorkerRPCAddress

	util.Debug(
		"worker running at %v entered WorkerPool\n",
		workerRPCAddress,
	)

	if _, ok := workerPool.workerStates[workerRPCAddress]; ok {
		// Worker is trying to re-register. Ignore.
		//
		// TODO(MEDIUM): this could be a place to allow recovery of a
		// worker?
		util.Debug("worker %v tried to re-register?\n", workerRPCAddress)
		return
	}

	// Record the new worker as free for a task.
	workerPool.workerStates[workerRPCAddress] = freeForTask

	// Try to assign a task to this worker.
	workerPool.sendOffMessage(
		newAssignTaskToWorkerMessage(workerRPCAddress),
	)
}
