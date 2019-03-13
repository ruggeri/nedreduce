package rpc

import (
	"strconv"

	"github.com/ruggeri/nedreduce/internal/mapper"
)

// MapTask is simply a wrapping of mapper.MapTask. The point is that
// this alias implements the Task interface.
type MapTask mapper.MapTask

// StartOnWorker asynchronously starts the MapTask on a remote worker.
func (mapTask *MapTask) StartOnWorker(
	workerAddress string,
	rpcCompletionCallback CompletionCallback,
) {
	go func() {
		err := Call(workerAddress, "Worker.ExecuteMapTask", mapTask, nil)

		rpcCompletionCallback(err)
	}()
}

// Identifier is a unique identifier for this task.
func (mapTask *MapTask) Identifier() string {
	return "map-task-" + strconv.Itoa(mapTask.MapTaskIdx)
}
