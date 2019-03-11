package rpc

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/reducer"
)

// ReduceTask is simply a wrapping of reducer.ReduceTask. The point is
// that this alias implements the Task interface.
type ReduceTask reducer.ReduceTask

// StartOnWorker asynchronously starts the ReduceTask on a remote
// worker.
func (reduceTask *ReduceTask) StartOnWorker(
	workerAddress string,
	rpcCompletionCallback CompletionCallback,
) {
	go func() {
		err := Call(
			workerAddress,
			"Worker.ExecuteReduceTask",
			reduceTask,
			nil,
		)

		if err != nil {
			log.Panicf(
				"Something went wrong with RPC call to worker: %v\n",
				err,
			)
		} else {
			rpcCompletionCallback()
		}
	}()
}
