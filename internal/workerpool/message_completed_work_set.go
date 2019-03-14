package workerpool

import (
	"github.com/ruggeri/nedreduce/internal/util"
)

type workSetCompletedMessage struct {
}

func newWorkSetCompletedMessage() *workSetCompletedMessage {
	return &workSetCompletedMessage{}
}

func (message *workSetCompletedMessage) Handle(
	workerPool *WorkerPool,
) {
	util.Debug("WorkerPool: work set has been completed\n")

	workerPool.currentWorkSetCh <- WorkerPoolCompletedWorkSet

	// We need to fire the condition variable because someone waiting to
	// submit a new job (or waiting to shut us down) needs to know that
	// they can try now.
	func() {
		workerPool.currentWorkSet = nil
		workerPool.cond.Broadcast()
	}()
}
