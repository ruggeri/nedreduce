package workerpool

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

// sendOffMessage asynchronously sends the given message. This method
// can be a little dangerous: you better not call it if there's a chance
// the WorkerPool might get shut down (e.g., when we are in shutting
// down mode).
//
// I suppose I could check the run state here. But the truth is that you
// can't hold the mutex while you send the message, so you'd only be
// checking the run state *before* message sending.
//
// Which isn't entirely safe. So you have to know what you're doing when
// you sendOffMessage.
func (workerPool *WorkerPool) sendOffMessage(message message) {
	// Record that there is another message in flight before starting the
	// async send.
	workerPool.noMoreMessagesWaitGroup.Add(1)
	go func() { workerPool.messageChannel <- message }()
}
