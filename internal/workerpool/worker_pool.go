package workerpool

import (
	"log"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// A WorkerPool manages a bunch of workers. Workers can register
// whenever they want to. Work sets can be submitted anytime you want;
// they'll be processed one-by-one though.
type WorkerPool struct {
	// Coordination
	//
	// messageChannel is an internal message channel.
	messageChannel messageChannel
	// noMoreMessagesWaitGroup lets you wait for all pending messages to
	// be cleared out of the messageChannel.
	noMoreMessagesWaitGroup sync.WaitGroup
	// mutex is used to coordinate changes to foreground state.
	mutex sync.Mutex
	// cond is used to signal changes to foreground state.
	cond *sync.Cond

	// Foreground state
	//
	// currentWorkSet is the currently assigned set of work. It is only
	// set in the background (where it doesn't need synchronization when
	// reading, because single-threaded), but it is *read* in the
	// foreground (which needs synchronization when reading). We need to
	// synchronize in the background when we write, of course.
	currentWorkSet *workSet
	// currenWorkSetCh is how we communicate events (like work set start,
	// work set completion) to the user of the WorkerPool. It has the same
	// synchronization story as currentWorkSet.
	currentWorkSetCh chan Event
	// runState records whether the WorkerPool is running, trying to shut
	// down, or shut down. Since it is modified by external methods, we
	// must synchronize when using this anywhere.
	runState workerPoolRunState

	// Background state. Background state changes happens only in the
	// singled-threaded background so no coordination is needed.
	//
	// workerStates records all connected workers.
	workerStates map[string]workerState
}

// Start starts an empty WorkerPool with no presently assigned task.
func Start() *WorkerPool {
	workerPool := &WorkerPool{
		messageChannel:          make(messageChannel),
		noMoreMessagesWaitGroup: sync.WaitGroup{},
		mutex:                   sync.Mutex{},
		cond:                    nil,

		currentWorkSet:   nil,
		currentWorkSetCh: nil,
		runState:         workerPoolIsRunning,

		workerStates: make(map[string]workerState),
	}

	workerPool.cond = sync.NewCond(&workerPool.mutex)

	go workerPool.handleMessages()

	return workerPool
}

// BeginNewWorkSet will asynchronously start the submitted tasks. The
// submission may fail to start if the WorkerPool gets shut down. We'll
// notify the caller of progress via the channel; they'll get messages
// if the work set couldn't begin, when the work set begins, and when
// the work set completes.
func (workerPool *WorkerPool) BeginNewWorkSet(
	tasks []mr_rpc.Task,
) chan Event {
	ch := make(chan Event)

	// We'll async start trying to ask the background thread to start the
	// WorkSet.
	message := newBeginNewWorkSetMessage(tasks, ch)
	go workerPool.tryToSendBeginNewWorkSetMessage(message)

	return ch
}

// RegisterNewWorker notifies the WorkerPool that a new worker is
// available for tasks. This method briefly acquires a mutex, but is
// effectively non-blocking.
func (workerPool *WorkerPool) RegisterNewWorker(
	workerRPCAddress string,
) {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	switch workerPool.runState {
	case workerPoolIsRunning, workerPoolShutDownIsRequested:
		// A new worker could help out. Note that even if shut down is
		// requested, we're still allowed to send registration messages.
		// Anyone shutting down hasn't yet gotten to the point where they're
		// waiting on the noMoreMessagesWaitGroup yet.
		workerPool.sendOffMessage(newRegisterWorkerMessage(
			workerRPCAddress,
		))
		return
	case workerPoolIsShuttingDown:
		// workerPoolIsShuttingDown is set when there is no currentWorkSet,
		// so we can't help presently. Also, no new work sets are started
		// after the WorkerPool starts shutting down. Therefore it is
		// pointless to send a message to the background thread to add the
		// worker. It will never be used.
		return
	case workerPoolIsShutDown:
		// There won't be any more work sets, and the background thread is
		// closed anyway. It would be fatal to send a message now.
		return
	default:
		log.Panicf(
			"Unexpected workerPoolRunState: %v\n",
			workerPool.runState,
		)
	}
}

// Shutdown tells the WorkerPool to stop processing new work sets. It
// will shut down the background goroutine. The method is synchronous:
// when we return, the WorkerPool is fully shut down.
func (workerPool *WorkerPool) Shutdown() {
	// Step 1: wait until there is no current work, and then start
	// shutting down the worker pool.
	func() {
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		for {
			switch workerPool.runState {
			case workerPoolIsRunning:
				if workerPool.currentWorkSet == nil {
					// Change state so that (1) no new jobs start, (2) no more
					// registrations are sent. We can't have any new messages
					// coming in, because below we're going to wait for messages
					// to clear out. Message count must go one way: down.
					workerPool.runState = workerPoolIsShuttingDown
					return
				}

				// We don't want to block new worker registrations (which could
				// help out the current work set). But we do want to block new
				// work sets from starting.
				workerPool.runState = workerPoolShutDownIsRequested
			case workerPoolIsShuttingDown:
				// Apparently someone else has started the shutdown. Cool. Let's
				// break this loop.
				return
			case workerPoolIsShutDown:
				// Someone finished the whole shutdown! Nothing left to do :-)
				return
			}

			workerPool.cond.Wait()
		}
	}()

	// There's no harm in going through this process of shutdown multiple
	// times. We can do it even if someone else has already fully shut
	// down the worker pool.
	//
	// Wait until all any in-flight worker registrations flush out.
	workerPool.noMoreMessagesWaitGroup.Wait()

	func() {
		// At this point, the runState can only change to
		// workerPoolIsShutDown, so the locking is probably superfluous.
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		if workerPool.runState == workerPoolIsShutDown {
			// Someone else did the shutdown for us. Cool.
			return
		}

		// Else we are the ones who are responsible for the shut down.
		workerPool.runState = workerPoolIsShutDown
	}()
}
