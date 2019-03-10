package rpc

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/reducer"
)

type ReduceTask reducer.ReduceTask

func (reduceTask *ReduceTask) StartOnWorker(
	workerAddress string,
	rpcCompletionCallback RPCCompletionCallback,
) {
	ok := Call(workerAddress, "Worker.ExecuteReduceTask", workerAddress, nil)

	if !ok {
		log.Fatal("Something went wrong with RPC call to worker.")
	} else {
		rpcCompletionCallback()
	}
}
