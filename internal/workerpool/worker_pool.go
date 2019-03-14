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
	// currentWorkSet is the currently assigned set of work.
	currentWorkSet *workSet
	// currenWorkSetCh is how we communicate events (like work set start,
	// work set completion) to the user of the WorkerPool.
	currentWorkSetCh chan WorkerPoolEvent
	// runState records whether the WorkerPool is running, trying to shut
	// down, or shut down.
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
) chan WorkerPoolEvent {
	ch := make(chan WorkerPoolEvent)

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
	case workerPoolIsRunning:
		// A new worker could help out.
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
		// Background thread is closed because we are shut down. There isn't
		// even a background thread to notify.
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
					// registrations are sent.
					workerPool.runState = workerPoolIsShuttingDown
					return
				}
				// We have to wait until there is no work set running.
				//
				// TODO(MEDIUM): Could we at least stop other work sets from
				// starting? Maybe a workerPoolShutdownIsRequested?
				//
				// If we just set workerPoolIsShuttingDown here, then no workers
				// could register anymore. And it isn't safe to continue to
				// register workers when in workerPoolIsShuttingDown, since as
				// soon as there are noMoreMessages then the background thread
				// can be stopped at any time.
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
	// times. We do it even if someone else has shut down the worker pool.
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
