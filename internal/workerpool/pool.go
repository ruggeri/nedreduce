package workerpool

import (
	"log"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// A WorkerPool manages a bunch of workers. Workers can register
// whenever they want to. The same WorkerPool can be assigned successive
// collection tasks. Each collection must finish entirely before the
// next can be started.
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
	// runState records whether the WorkerPool is running, trying to shut
	// down, or shut down.
	runState workerPoolRunState
	// currentWorkSet is the currently assigned set of work.
	currentWorkSet   *workSet
	currentWorkSetCh chan WorkerPoolEvent

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

		currentWorkSet:   nil,
		currentWorkSetCh: nil,
		runState:         workerPoolIsRunning,
		mutex:            sync.Mutex{},
		cond:             nil,
		workerStates:     make(map[string]workerState),
	}

	workerPool.cond = sync.NewCond(&workerPool.mutex)

	go workerPool.handleMessages()

	return workerPool
}

// BeginNewWorkSet synchronously waits until the pool is free. It then
// kicks off the work set to be performed. It doesn't wait for the work
// set to complete; it returns a channel so that the user can decide
// whether/when they want to wait.
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

// RegisterNewWorker asynchronously notifies the pool manager that a new
// worker has connected.
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
		// workerPoolIsShuttingDown is set when there is no
		// currentWorkSet. There never will be after it is set. Therefore
		// it is pointless to send a message to the background thread to
		// add the worker.
		return
	case workerPoolIsShutDown:
		// Background thread is closed because we are shut down. No
		// background thread to notify.
		return
	default:
		log.Panicf(
			"Unexpected workerPoolRunState: %v\n",
			workerPool.runState,
		)
	}
}

// Shutdown tells the WorkerPool to stop processing new messages. It
// synchronously waits for all the pending messages to clear out. It
// terminates the background goroutine.
func (workerPool *WorkerPool) Shutdown() {
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

	// Wait until all any in-flight worker registrations flush out.
	workerPool.noMoreMessagesWaitGroup.Wait()

	func() {
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
