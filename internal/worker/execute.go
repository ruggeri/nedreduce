package worker

import (
	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

func (wk *Worker) ExecuteMapTask(mapTask *mr_rpc.MapTask, _ *struct{}) error {
	return wk.DoTask(func() {
		mapper.ExecuteMapping((*mapper.MapTask)(mapTask))
	})
}

func (wk *Worker) ExecuteReduceTask(reduceTask *mr_rpc.ReduceTask, _ *struct{}) error {
	return wk.DoTask(func() {
		reducer.ExecuteReducing((*reducer.ReduceTask)(reduceTask))
	})
}
