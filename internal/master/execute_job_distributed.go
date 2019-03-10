package master

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runDistributedMapPhase runs a distributed map phase on the master.
func runDistributedMapPhase(master *Master) {
	// Boilerplate to cast []MapTask to []mr_rpc.Task.
	allTasks := []mr_rpc.Task(nil)
	for _, mapTask := range mapper.AllMapTasks(master.jobConfiguration) {
		mapTask := mr_rpc.MapTask(mapTask)
		allTasks = append(allTasks, mr_rpc.Task(&mapTask))
	}

	// The worker pool will hand out the map tasks to the workers.
	workSetResultChan, err := master.workerPool.AssignNewWorkSet(allTasks)

	// In theory the WorkerPool could have been shut down before we could
	// assign the work.
	if err != nil {
		log.Panic("WorkerPool wasn't able to start mapPhase?")
	}

	// We wait until the WorkerPool has completed all the work.
	<-workSetResultChan
}

// runDistributedReducePhase runs a distributed reduce phase on the
// master.
func runDistributedReducePhase(master *Master) {
	// Boilerplate to cast []ReduceTask to []mr_rpc.Task.
	allTasks := []mr_rpc.Task(nil)
	for _, reduceTask := range reducer.AllReduceTasks(master.jobConfiguration) {
		reduceTask := mr_rpc.ReduceTask(reduceTask)
		allTasks = append(allTasks, mr_rpc.Task(&reduceTask))
	}

	// The worker pool will hand out the reduce tasks to the workers.
	workSetResultChan, err := master.workerPool.AssignNewWorkSet(allTasks)

	// In theory the WorkerPool could have been shut down before we could
	// assign the work.
	if err != nil {
		log.Panic("WorkerPool wasn't able to start reducePhase?")
	}

	// We wait until the WorkerPool has completed all the work.
	<-workSetResultChan
}

// StartDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func StartDistributedJob(
	jobConfiguration *types.JobConfiguration,
	masterAddress string,
) *Master {
	// First construct the Master and start running an RPC server which
	// can listen for connections.
	master := StartMaster(masterAddress, jobConfiguration)

	// In the background, begin executing the job.
	go master.executeJob(
		runDistributedMapPhase,
		runDistributedReducePhase,
	)

	return master
}
