package jobcoordinator

// TODO(HIGH): what's the plan with killWorkers?

// //
// // Please do not modify this file.
// //

// import (
// 	"fmt"

// 	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
// 	"github.com/ruggeri/nedreduce/internal/util"
// )

// // killWorkers cleans up all workers by sending each one a Shutdown RPC.
// // It also collects and returns the number of tasks each worker has
// // performed.
// func (master *Master) killWorkers() []int {
// 	master.Lock()
// 	defer master.Unlock()

// 	workerRPCAddresses := master.workerPoolManager.WorkerRPCAddresses()
// 	numTasksProcessed := make([]int, 0, len(workerRPCAddresses))
// 	for _, w := range workerRPCAddresses {
// 		util.Debug("Master: shutdown worker %s\n", w)
// 		var reply mr_rpc.ShutdownReply
// 		ok := mr_rpc.Call(w, "Worker.Shutdown", new(struct{}), &reply)
// 		if ok == false {
// 			fmt.Printf("Master: RPC %s shutdown error\n", w)
// 		} else {
// 			numTasksProcessed = append(numTasksProcessed, reply.NumTasksProcessed)
// 		}
// 	}
// 	return numTasksProcessed
// }
