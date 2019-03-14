package workerpool

import (
	"github.com/ruggeri/nedreduce/internal/util"
)

// A workSetCompletedMessage is fired when all the tasks of a work set
// are done.
type workSetCompletedMessage struct {
}

func newWorkSetCompletedMessage() *workSetCompletedMessage {
	return &workSetCompletedMessage{}
}

func (message *workSetCompletedMessage) Handle(
	workerPool *WorkerPool,
) {
	util.Debug("WorkerPool: work set has been completed\n")

	// Notify the user that we have completed their work.
	workerPool.currentWorkSetCh <- CompletedWorkSet

	// We need to fire the condition variable because someone waiting to
	// submit a new job (or waiting to shut us down) needs to know that
	// they can try now.
	//
	// Subtlety: it is important to lock here. We don't want someone to
	// observe currentWorkSet != nil, then we Broadcast, then they wait.
	func() {
		workerPool.mutex.Lock()
		defer workerPool.mutex.Unlock()

		workerPool.currentWorkSet = nil
		workerPool.cond.Broadcast()
	}()
}
