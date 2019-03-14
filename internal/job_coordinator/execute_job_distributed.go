package job_coordinator

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/workerpool"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	"github.com/ruggeri/nedreduce/internal/types"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// runDistributedMapPhase runs a distributed map phase on the
// jobCoordinator.
func runDistributedMapPhase(
	jobCoordinator *JobCoordinator,
	jobConfiguration *types.JobConfiguration,
) {
	// Boilerplate to cast []MapTask to []mr_rpc.Task.
	allTasks := []mr_rpc.Task(nil)
	for _, mapTask := range mapper.AllMapTasks(jobConfiguration) {
		mapTask := mr_rpc.MapTask(mapTask)
		allTasks = append(allTasks, mr_rpc.Task(&mapTask))
	}

	// The worker pool will hand out the map tasks to the workers.
	workSetEvents := jobCoordinator.workerPool.BeginNewWorkSet(allTasks)

	// Wait for commencement.
	switch event := <-workSetEvents; event {
	case workerpool.CommencedWorkSet:
		// Good.
	default:
		log.Panicf("Expected WorkerPool to commence work. Got: %v\n", event)
	}

	// Wait for completion.
	switch event := <-workSetEvents; event {
	case workerpool.CompletedWorkSet:
		// Good.
	default:
		log.Panicf("Expected WorkerPool to complete work. Got: %v\n", event)
	}
}

// runDistributedReducePhase runs a distributed reduce phase on the
// jobCoordinator.
func runDistributedReducePhase(
	jobCoordinator *JobCoordinator,
	jobConfiguration *types.JobConfiguration,
) {
	// Boilerplate to cast []ReduceTask to []mr_rpc.Task.
	allTasks := []mr_rpc.Task(nil)
	for _, reduceTask := range reducer.AllReduceTasks(jobConfiguration) {
		reduceTask := mr_rpc.ReduceTask(reduceTask)
		allTasks = append(allTasks, mr_rpc.Task(&reduceTask))
	}

	// The worker pool will hand out the reduce tasks to the workers.
	workSetEvents := jobCoordinator.workerPool.BeginNewWorkSet(allTasks)

	// Wait for commencement.
	switch event := <-workSetEvents; event {
	case workerpool.CommencedWorkSet:
		// Good.
	default:
		log.Panicf("Expected WorkerPool to commence work. Got: %v\n", event)
	}

	// Wait for completion.
	switch event := <-workSetEvents; event {
	case workerpool.CompletedWorkSet:
		// Good.
	default:
		log.Panicf("Expected WorkerPool to complete work. Got: %v\n", event)
	}
}
