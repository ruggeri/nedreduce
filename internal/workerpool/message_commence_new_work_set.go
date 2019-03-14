package workerpool

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/util"
)

type commenceNewWorkSetMessage struct {
}

func newCommenceNewWorkSetMessage() *commenceNewWorkSetMessage {
	return &commenceNewWorkSetMessage{}
}

// handleStartingNewWorkSet starts working on a workSet by assigning
// tasks to all currently registered workers.
func (message *commenceNewWorkSetMessage) Handle(workerPool *WorkerPool) {
	util.Debug("WorkerPool is commencing work on a new work set\n")

	workerPool.currentWorkSetCh <- WorkerPoolCommencedWorkSet

	// Hand out the initial tasks!
	for workerRPCAddress, workerState := range workerPool.workerStates {
		switch workerState {
		case failed:
			// Skip failed workers.
		case workingOnTask:
			log.Panic("How is worker already working on a task?\n")
		case freeForTask:
			workerPool.sendOffMessage(
				newAssignTaskToWorkerMessage(workerRPCAddress),
			)
		default:
			log.Panicf("Unexpected workerState: %v\n", workerState)
		}
	}
}
