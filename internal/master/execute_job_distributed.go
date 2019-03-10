package master

import (
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
)

// runDistributedMapPhase runs a distributed map phase on the master.
func runDistributedMapPhase(master *Master) {
	mapTasksIterator := mapper.NewMapTasksIterator(master.jobConfiguration)

	// This function will produce each map task one-by-one.
	produceNextMapTask := func() (mr_rpc.Task, error) {
		mapTask, err := mapTasksIterator.Next()

		if err == io.EOF {
			return nil, io.EOF
		} else if err != nil {
			log.Panicf("Unexpected MapTask iteration error: %v\n", err)
		}

		return mr_rpc.Task((*mr_rpc.MapTask)(mapTask)), nil
	}

	// The worker pool will hand out the map tasks to the workers.
	workSetResultChan := master.workerPool.AssignNewWorkSet(produceNextMapTask)

	// We wait until the workAssigner has completed all the work.
	<-workSetResultChan
}

// runDistributedReducePhase runs a distributed reduce phase on the
// master.
func runDistributedReducePhase(master *Master) {
	reduceTasksIterator := reducer.NewReduceTasksIterator(master.jobConfiguration)

	// This function will produce each reduce task one-by-one.
	produceNextReduceTask := func() (mr_rpc.Task, error) {
		reduceTask, err := reduceTasksIterator.Next()

		if err == io.EOF {
			return nil, io.EOF
		} else if err != nil {
			log.Panicf("Unexpected ReduceTask iteration error: %v\n", err)
		}

		return mr_rpc.Task((*mr_rpc.ReduceTask)(reduceTask)), nil
	}

	// The worker pool will hand out the reduce tasks to the workers.
	workSetResultChan := master.workerPool.AssignNewWorkSet(produceNextReduceTask)

	// We wait until the workAssigner has completed all the work.
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
