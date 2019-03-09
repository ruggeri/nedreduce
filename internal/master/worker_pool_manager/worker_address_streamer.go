package worker_pool_manager

// workerAddressStreamer is a helper that streams worker RPC addresses
// down a channel.
type workerAddressStreamer struct {
	manager *WorkerPoolManager
	// Track how many worker addresses we have sent.
	numWorkersSent            int
	workerRPCAddressesChannel chan string
}

func makeWorkerRPCAddressStream(manager *WorkerPoolManager) chan string {
	streamer := &workerAddressStreamer{
		manager:                   manager,
		numWorkersSent:            0,
		workerRPCAddressesChannel: make(chan string),
	}

	// Start the streamer in a background goroutine.
	go streamer.run()

	// Return the channel it will pipe the worker RPC addresses down.
	return streamer.workerRPCAddressesChannel
}

// This method is run in a background goroutine to push worker RPC
// addresses down a channel.
func (streamer *workerAddressStreamer) run() {
	// Lock the manager so the state can't change on us while we check it
	// out.
	streamer.manager.mutex.Lock()
	defer streamer.manager.mutex.Unlock()

	for {
		if streamer.manager.state == shutdown {
			// When the manager is closed, all workers will be released from
			// the pool, and therefore we shouldn't bother sending those. Let
			// the user know we are done by closing the stream.
			close(streamer.workerRPCAddressesChannel)
			// The streamer has nothing left to do. It exits.
			return
		} else if streamer.numWorkersSent < len(streamer.manager.workerRPCAddresses) {
			// There are new workers to stream on down. Handle them. (Note:
			// handleNewWorkers will unlock the manager's mutex.)
			streamer.handleNewWorkers()

			// Since handleNewWorkers unlocked the mutex on the manager's
			// internal state, the manager may have accepted even more new
			// workers while we were doing our streaming. To check for that
			// possibility, first relock the mutex...
			streamer.manager.mutex.Lock()
			// And jump to the top of the loop to double-check whether
			// anything has happened while we were streaming.
			continue
		} else {
			// Nothing has happened since last time, so there is nothing to
			// do. Therefore, pause here until the manager tells us some new
			// events have occurred.
			//
			// Condition variables release the mutex while waiting, so waiting
			// here won't block the manager. On wake, the condition variable
			// reacquires the mutex. It's important that the mutex is
			// reacquired so that we can safely inspect the manager's internal
			// state once again when we loop back around.
			streamer.manager.conditionVariable.Wait()
		}
	}

	// This should be unreachable.
}

// handleNewWorkers streams down new worker RPC addresses to the user.
// Warning: this function releases the lock on the manager's state!
func (streamer *workerAddressStreamer) handleNewWorkers() {
	// Get the new workers to send on down.
	workersToSend := streamer.manager.workerRPCAddresses[streamer.numWorkersSent:]
	// Be careful and make a "defensive copy." We don't want to hold on to
	// any of the manager's internal state, since we're about to unlock
	// the manager.
	workersToSend = append([]string(nil), workersToSend...)

	// We will send the workers' RPC addresses down a channel, which is a
	// *blocking* operation. We don't want to block the manager while we
	// send them. Therefore, we unlock the mutex on the manager.
	streamer.manager.mutex.Unlock()

	// Now send each of the new workers down the channel.
	for _, workerToSend := range workersToSend {
		streamer.workerRPCAddressesChannel <- workerToSend
		streamer.numWorkersSent++
	}
}
