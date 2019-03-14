package workerpool

import "log"

// A message is a unit of work for the background thread to execute.
type message interface {
	Handle(workerPool *WorkerPool)
}

// The messageChannel is used by the background thread of the WorkerPool
// to handle messages one at a time. The goal is (was?) to simplify the
// WorkerPool, since most messages don't need to lock.
type messageChannel chan message

// handleMessages is run by a background goroutine to handle
// successive incoming messages.
func (workerPool *WorkerPool) handleMessages() {
	for message := range workerPool.messageChannel {
		// Handle the message.
		message.Handle(workerPool)

		// The message is processed, so there is one less in flight.
		workerPool.noMoreMessagesWaitGroup.Done()
	}
}

// sendOffMessage asynchronously sends the given message.
func (workerPool *WorkerPool) sendOffMessage(message message) {
	switch workerPool.runState {
	case workerPoolIsShuttingDown:
		// TODO(MEDIUM): I don't think I need this? Test without? Feels
		// dangerous anyway because uncoordinated read of the runState...
		//
		// After shut down begins, start dropping all messages.
		return
	case workerPoolIsShutDown:
		log.Panicf(
			"message sould not be sent after WorkerPool shut down:%v\n",
			message,
		)
	}

	// Record that there is another message in flight before starting the
	// async send.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() { workerPool.messageChannel <- message }()
}
