package workerpool

import (
	"log"
	"os"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// A WorkerPool manages a bunch of workers. Workers can register
// whenever they want to. The same WorkerPool can be assigned successive
// collection tasks. Each collection must finish entirely before the
// next can be started.
type WorkerPool struct {
	// messageChannel is an internal message channel.
	messageChannel messageChannel
	// noMoreMessagesWaitGroup lets you wait for all pending messages to
	// be cleared out of the messageChannel.
	noMoreMessagesWaitGroup sync.WaitGroup

	// mutex protects changes to mutable state.
	mutex               sync.Mutex
	runStateChangedCond *sync.Cond

	// Mutable state
	//
	// currentWorkSet is the currently assigned set of work.
	currentWorkSet   *workSet
	currentWorkSetCh chan WorkerPoolEvent
	// runState records whether the WorkerPool is running, trying to shut
	// down, or shut down.
	runState workerPoolRunState
	// workerStates records all connected workers.
	workerStates map[string]workerState
}

// Start starts an empty WorkerPool with no presently assigned task.
func Start() *WorkerPool {
	workerPool := &WorkerPool{
		messageChannel: make(messageChannel),
		// noMoreMessagesWaitGroup: sync.WaitGroup{},

		mutex: sync.Mutex{},
		// the condition variable cannot be set up until after the mutex is
		// constructed.
		runStateChangedCond: nil,

		currentWorkSet:   nil,
		currentWorkSetCh: nil,
		runState:         freeForWorkSet,
		workerStates:     make(map[string]workerState),
	}

	workerPool.runStateChangedCond = sync.NewCond(&workerPool.mutex)

	go workerPool.handleMessages()

	return workerPool
}

// BeginNewWorkSet synchronously waits until the pool is free. It then
// kicks off the work set to be performed. It doesn't wait for the work
// set to complete; it returns a channel so that the user can decide
// whether/when they want to wait.
func (workerPool *WorkerPool) BeginNewWorkSet(
	tasks []mr_rpc.Task,
) (chan WorkerPoolEvent, error) {
	workSet := newWorkSet(tasks)
	ch := make(chan WorkerPoolEvent)

	workerPool.sendOffMessage(newBeginNewWorkSetMessage(workSet, ch))

	switch event := <-ch; event {
	case WorkerPoolIsBusy, WorkerPoolIsShuttingDown, WorkerPoolIsShutDown:
		return nil, os.ErrClosed
	case WorkerPoolHasBegunNewWorkSet:
		return ch, nil
	default:
		log.Panicf("Unexpected event: %v\n", event)
	}

	panic("unreachable")
}

// RegisterNewWorker asynchronously notifies the pool manager that a new
// worker has connected.
func (workerPool *WorkerPool) RegisterNewWorker(
	workerRPCAddress string,
) {
	workerPool.sendOffMessage(newRegisterWorkerMessage(
		workerRPCAddress,
	))
}

// Shutdown tells the WorkerPool to stop processing new messages. It
// synchronously waits for all the pending messages to clear out. It
// terminates the background goroutine.
func (workerPool *WorkerPool) Shutdown() {
	ch := make(chan WorkerPoolEvent)
	workerPool.sendOffMessage(newBeginShutdownMessage(ch))

	// Now wait for the shutdown to complete.
	<-ch
}
