package workerpool

import (
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// beginNewWorkSetMessage is sent when we want to try to start a new
// work set.
type beginNewWorkSetMessage struct {
	Tasks []mr_rpc.Task
	Ch    chan Event
}

func newBeginNewWorkSetMessage(
	tasks []mr_rpc.Task,
	ch chan Event,
) *beginNewWorkSetMessage {
	return &beginNewWorkSetMessage{
		Tasks: tasks,
		Ch:    ch,
	}
}

func (message *beginNewWorkSetMessage) Handle(workerPool *WorkerPool) {
	// We have to lock because (1) we're reading runState (which can be
	// changed in the foreground methods) and (2) we're writing
	// currentWorkSet (which can be viewed in the foreground).
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	// Is the WorkerPool capable of starting this new work set right now?
	if workerPool.runState != workerPoolIsRunning || workerPool.currentWorkSet != nil {
		// If not, we need to block until something changes. We'll do that
		// in a goroutine, because we can't block the background worker
		// thread.
		go workerPool.tryToSendBeginNewWorkSetMessage(message)
		return
	}

	util.Debug("WorkerPool is commencing work on a new work set\n")

	// Update the current work set.
	workerPool.currentWorkSet = newWorkSet(message.Tasks)
	workerPool.currentWorkSetCh = message.Ch

	// Notify the user that we have started their job.
	workerPool.currentWorkSetCh <- CommencedWorkSet

	// Hand out the initial tasks!
	for workerRPCAddress, workerState := range workerPool.workerStates {
		switch workerState {
		case failed:
			// Skip failed workers.
		case workingOnTask:
			// Sanity check.
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

// tryToSendBeginNewWorkSetMessage will wait until it think it has a
// chance of successfully beginning a new work set. When that happens,
// it will send a message to the background thread.
func (workerPool *WorkerPool) tryToSendBeginNewWorkSetMessage(
	message *beginNewWorkSetMessage,
) {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	for {
		switch workerPool.runState {
		case workerPoolIsRunning:
			if workerPool.currentWorkSet == nil {
				// We can try to schedule a new job! Maybe it will work!
				workerPool.sendOffMessage(message)
				return
			}
			// Someone else is running a job. We'll wait until they are done.
		case workerPoolIsShuttingDown:
			// We won't start any new jobs after shut down begins.
			message.Ch <- DidNotAcceptWorkSet
			return
		case workerPoolIsShutDown:
			// Can't start work when the WorkerPool is shut down. In fact, it
			// would be fatal anyway, because the internal message channel is
			// closed.
			message.Ch <- DidNotAcceptWorkSet
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
