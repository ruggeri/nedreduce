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
	go func() {
		ok := Call(workerAddress, "Worker.ExecuteReduceTask", reduceTask, nil)

		if !ok {
			log.Fatal("Something went wrong with RPC call to worker.")
		} else {
			rpcCompletionCallback()
		}
	}()
}
