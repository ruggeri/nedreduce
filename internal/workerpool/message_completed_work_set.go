package workerpool

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/util"
)

type workSetCompletedMessage struct {
}

func newWorkSetCompletedMessage() *workSetCompletedMessage {
	return &workSetCompletedMessage{}
}

func (message *workSetCompletedMessage) Handle(
	workerPool *WorkerPool,
) {
	util.Debug("WorkerPool: work set has been completed\n")

	ch := workerPool.currentWorkSetCh
	go func() { ch <- WorkerPoolHasCompletedWorkSet }()

	func() {
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		workerPool.currentWorkSet = nil
		workerPool.currentWorkSetCh = nil

		switch workerPool.runState {
		case freeForWorkSet:
			log.Panicf("Can't be free for a work set if we just completed one...")
		case workingOnWorkSet:
			// Good. See below.
			workerPool.runState = freeForWorkSet
			workerPool.runStateChangedCond.Broadcast()
		case shuttingDown:
			// Great, let them keep shutting down.
			return
		case shutDown:
			log.Panicf("After shut down no messages should be delivered...")
		default:
			log.Panicf("Unexpected runState: %v\n", workerPool.runState)
		}
	}()
}
