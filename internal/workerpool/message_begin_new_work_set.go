package workerpool

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/util"
)

type beginNewWorkSetMessage struct {
	WorkSet *workSet
	Ch      chan WorkerPoolEvent
}

func newBeginNewWorkSetMessage(
	workSet *workSet,
	ch chan WorkerPoolEvent,
) *beginNewWorkSetMessage {
	return &beginNewWorkSetMessage{
		WorkSet: workSet,
		Ch:      ch,
	}
}

// handleStartingNewWorkSet starts working on a workSet by assigning
// tasks to all currently registered workers.
func (message *beginNewWorkSetMessage) Handle(workerPool *WorkerPool) {
	didStartWorkSet := func() bool {
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		switch workerPool.runState {
		case freeForWorkSet:
			// Great! See below!
		case workingOnWorkSet:
			go func() { message.Ch <- WorkerPoolIsBusy }()
			return false
		case shuttingDown:
			go func() { message.Ch <- WorkerPoolIsShuttingDown }()
			return false
		case shutDown:
			log.Panicf("After shut down no messages should be delivered...")
		default:
			log.Panicf("Unexpected WorkerPool runState: %v\n", workerPool.runState)
		}

		util.Debug("WorkerPool is starting a new work set\n")

		// Important to sync send this before we start the job, which will
		// reuse the channel to send a completion event. The messages must
		// be sent in order.
		message.Ch <- WorkerPoolHasBegunNewWorkSet

		workerPool.currentWorkSet = message.WorkSet
		workerPool.currentWorkSetCh = message.Ch
		workerPool.runState = workingOnWorkSet
		workerPool.runStateChangedCond.Broadcast()

		return true
	}()

	if !didStartWorkSet {
		return
	}

	// Hand out some tasks!
	for workerRPCAddress, workerState := range workerPool.workerStates {
		if workerState == freeForTask {
			workerPool.sendOffMessage(
				newAssignTaskToWorkerMessage(workerRPCAddress),
			)
		}
	}
}
