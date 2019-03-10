package worker

import mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
import "github.com/ruggeri/nedreduce/internal/util"

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *mr_rpc.WorkerShutdownResponse) error {
	util.Debug("Shutdown %s\n", wk.rpcAddress)
	wk.Lock()
	defer wk.Unlock()
	res.NumTasksProcessed = wk.nTasks
	wk.nRPC = 1
	return nil
}
