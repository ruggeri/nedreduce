package rpc

import (
	"strconv"

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

		rpcCompletionCallback(err)
	}()
}

func (reduceTask *ReduceTask) Identifier() string {
	return "reduce-task-" + strconv.Itoa(reduceTask.ReduceTaskIdx)
}
