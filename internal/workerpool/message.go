package workerpool

import "log"

type message interface {
	Handle(workerPool *WorkerPool)
}

// A messageChannel is a channel we can push internal messages over.
// This is used by the WorkerPool to notify us of newly registered
// workers, and it is also used when a worker has completed some work
// and wants to be assigned a new task.
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

func (workerPool *WorkerPool) sendOffMessage(message message) {
	switch workerPool.runState {
	case shuttingDown:
		// After shut down begins, start dropping all messages.
		return
	case shutDown:
		log.Panicf(
			"message sould not be sent after WorkerPool shut down:%v\n",
			message,
		)
	}

	// Record that there is another message in flight. That makes it safe
	// to send the message async.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() { workerPool.messageChannel <- message }()
}
