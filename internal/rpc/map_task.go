package rpc

import (
	"log"

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
		ok := Call(workerAddress, "Worker.ExecuteMapTask", mapTask, nil)

		if !ok {
			log.Fatal("Something went wrong with RPC call to worker.")
		} else {
			rpcCompletionCallback()
		}
	}()
}
