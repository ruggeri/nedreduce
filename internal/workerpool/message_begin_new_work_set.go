package workerpool

import (
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

type beginNewWorkSetMessage struct {
	Tasks []mr_rpc.Task
	Ch    chan WorkerPoolEvent
}

func newBeginNewWorkSetMessage(
	tasks []mr_rpc.Task,
	ch chan WorkerPoolEvent,
) *beginNewWorkSetMessage {
	return &beginNewWorkSetMessage{
		Tasks: tasks,
		Ch:    ch,
	}
}

// handleStartingNewWorkSet starts working on a workSet by assigning
// tasks to all currently registered workers.
func (message *beginNewWorkSetMessage) Handle(workerPool *WorkerPool) {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	if workerPool.runState != workerPoolIsRunning || workerPool.currentWorkSet != nil {
		// Move it off the background thread and we'll try again later
		// maybe.
		go workerPool.tryToSendBeginNewWorkSetMessage(message)
		return
	}

	util.Debug("WorkerPool is commencing work on a new work set\n")

	workerPool.currentWorkSet = newWorkSet(message.Tasks)
	workerPool.currentWorkSetCh = message.Ch

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

func (workerPool *WorkerPool) tryToSendBeginNewWorkSetMessage(
	message *beginNewWorkSetMessage,
) {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	for {
		switch workerPool.runState {
		case workerPoolIsRunning:
			if workerPool.currentWorkSet == nil {
				// We can try to schedule a new job!
				workerPool.sendOffMessage(message)
				return
			}
			// Someone else is running a job. We'll wait until they are done.
		case workerPoolIsShuttingDown:
			// We won't start any new jobs after shut down begins.
			message.Ch <- WorkerPoolDidNotAcceptWorkSet
			return
		case workerPoolIsShutDown:
			// Background thread is closed because the WorkerPool is shut
			// down. No background thread to even notify.
			message.Ch <- WorkerPoolDidNotAcceptWorkSet
			return
		default:
			log.Panicf(
				"Unexpected workerPoolRunState: %v\n",
				workerPool.runState,
			)
		}

		workerPool.cond.Wait()
	}
}
