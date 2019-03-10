package rpc

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/mapper"
)

type MapTask mapper.MapTask

func (mapTask *MapTask) StartOnWorker(
	workerAddress string,
	rpcCompletionCallback RPCCompletionCallback,
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
