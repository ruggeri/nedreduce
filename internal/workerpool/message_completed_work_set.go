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

	// TODO: Somehow we're supposed to notify the submitter of the job.

	func() {
		workerPool.currentWorkSet = nil
		workerPool.runStateCond.Broadcast()
	}()
}
