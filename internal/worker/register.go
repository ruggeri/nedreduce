package worker

import (
	"fmt"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(mr_rpc.WorkerRegistrationMessage)
	args.WorkerRPCAdress = wk.name
	ok := mr_rpc.Call(master, "JobCoordinator.RegisterWorker", args, new(struct{}))
	if ok == false {
		fmt.Printf("RegisterWorker: RPC %s register error\n", master)
	}
}
