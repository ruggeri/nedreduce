package workerpool

import "log"

type beginShutdownMessage struct {
	Ch chan WorkerPoolEvent
}

func newBeginShutdownMessage(
	ch chan WorkerPoolEvent,
) *beginShutdownMessage {
	return &beginShutdownMessage{
		Ch: ch,
	}
}

func (message *beginShutdownMessage) Handle(workerPool *WorkerPool) {
	workerPool.beginShuttingDown()

	// Wait for all queued messages to clear out.
	workerPool.noMoreMessagesWaitGroup.Wait()

	func() {
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		if workerPool.runState == shutDown {
			// how nice, someone has shut down for us already.
			return
		} else if workerPool.runState != shuttingDown {
			log.Panicf("Unexpected run state at shut down: %v\n", workerPool.runState)
		}

		// Okay, let's shut it down!
		close(workerPool.messageChannel)

		workerPool.runState = shutDown
		workerPool.runStateChangedCond.Broadcast()
	}()

	message.Ch <- WorkerPoolIsShutDown
}

func (workerPool *WorkerPool) beginShuttingDown() {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	for {
		switch workerPool.runState {
		case freeForWorkSet:
			// Great! No work assigned is the perfect time to shut down.
			break
		case workingOnWorkSet:
			// we'll have to keep waiting...
		case shuttingDown:
			// someone already kicked off shut down.
			return
		case shutDown:
			// someone already finished the shut down.
			return
		}
		if workerPool.runState == freeForWorkSet {
			break
		}

		workerPool.runStateChangedCond.Wait()
	}

	// We can now update the runState to shutting down.
	workerPool.runState = shuttingDown
	workerPool.runStateChangedCond.Broadcast()

	return
}
