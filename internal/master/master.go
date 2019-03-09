package master

//
// Please do not modify this file.
//

import (
	"fmt"
	"log"
	"sync"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/master/worker_pool_manager"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// Master holds all the state that the master needs to keep track of.
type Master struct {
	sync.Mutex

	Address     string
	DoneChannel chan bool

	workerPoolManager *worker_pool_manager.WorkerPoolManager
	rpcServer         *masterRPCServer

	// protected by the mutex
	JobConfiguration *types.JobConfiguration
	Stats            []int
}

// newMaster initializes a new Map/Reduce Master
func newMaster(
	masterAddress string,
	jobConfiguration *types.JobConfiguration,
) (master *Master) {
	master = new(Master)
	master.Address = masterAddress
	master.workerPoolManager = worker_pool_manager.StartManager()
	master.DoneChannel = make(chan bool)

	master.JobConfiguration = jobConfiguration
	return
}

// RunSequentialJob runs map and reduce tasks sequentially, waiting for
// each task to complete before running the next.
func RunSequentialJob(
	jobConfiguration *types.JobConfiguration,
) (master *Master) {
	master = newMaster("master", jobConfiguration)

	// Master's coordination/execution of the MapReduce job will run in a
	// background thread.
	go master.runJob(
		jobConfiguration,
		// This function executes each of the two phases.
		func(jobPhase types.JobPhase) {
			switch jobPhase {
			case types.MapPhase:
				util.Debug("Beginning mapping phase\n")
				// Run each map task one-by-one.
				for mapTaskIdx := 0; mapTaskIdx < jobConfiguration.NumMappers(); mapTaskIdx++ {
					mapperConfiguration := mapper.ConfigurationFromJobConfiguration(jobConfiguration, mapTaskIdx)
					mapper.ExecuteMapping(&mapperConfiguration)
				}
			case types.ReducePhase:
				// Run each reduce task one-by-one.
				util.Debug("Beginning reducing phase\n")
				for reduceTaskIdx := 0; reduceTaskIdx < jobConfiguration.NumReducers; reduceTaskIdx++ {
					reducerConfiguration := reducer.ConfigurationFromJobConfiguration(jobConfiguration, reduceTaskIdx)
					reducer.ExecuteReducing(&reducerConfiguration)
				}
			}
		},
		// This function collects stats when both phases are complete.
		func() {
			master.Stats = []int{len(jobConfiguration.MapperInputFileNames) + jobConfiguration.NumReducers}
		},
	)

	return
}

func (master *Master) startRPCServer() {
	if master.rpcServer != nil {
		log.Fatalf("Trying to start master's RPC server twice?")
	} else {
		master.rpcServer = startMasterRPCServer(master)
	}
}

func (master *Master) stopRPCServer() {
	if master.rpcServer == nil {
		log.Fatalf("Trying to stop an RPC server that was never started?")
	} else {
		master.rpcServer.Shutdown()
		master.rpcServer = nil
	}
}

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func RunDistributedJob(
	jobConfiguration *types.JobConfiguration,
	masterAddress string,
) (master *Master) {
	// First construct the Master and start running an RPC server which
	// can listen for connections.
	master = newMaster(masterAddress, jobConfiguration)
	master.startRPCServer()

	go master.runJob(
		jobConfiguration,
		// This function is used to execute each job phase.
		func(jobPhase types.JobPhase) {
			// Start running someone to listen for workers to register with
			// the master. As workers register, we will add them to our pool
			// of available workers.
			workerRegistrationChannel := master.workerPoolManager.WorkerRPCAddressStream()

			runDistributedPhase(
				jobConfiguration,
				jobPhase,
				workerRegistrationChannel,
			)
		},
		// This function is run after all work is complete. The workers are
		// told to shut down, and we collect up all their stats. Last, we
		// stop running the RPC server.
		func() {
			master.Stats = master.killWorkers()
			master.stopRPCServer()
		})
	return
}

// Wait blocks until the currently scheduled work has completed. This
// happens when all tasks have been scheduled and completed, the final
// output have been computed, and all workers have been shut down.
func (master *Master) Wait() {
	<-master.DoneChannel
}

// runJob executes a mapreduce job on the given number of mappers and
// reducers.
func (master *Master) runJob(
	jobConfiguration *types.JobConfiguration,
	runPhase func(phase types.JobPhase),
	collectStatsAndCleanup func(),
) {
	fmt.Printf("%s: Starting Map/Reduce task %s\n", master.Address, jobConfiguration.JobName)

	runPhase(types.MapPhase)
	runPhase(types.ReducePhase)
	collectStatsAndCleanup()
	util.MergeReducerOutputFiles(jobConfiguration.JobName, jobConfiguration.NumReducers)

	fmt.Printf("%s: Map/Reduce task completed\n", master.Address)

	master.DoneChannel <- true
}

func (master *Master) Shutdown() {
	master.rpcServer.Shutdown()
	master.workerPoolManager.SendShutdown()
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has
// performed.
func (master *Master) killWorkers() []int {
	master.Lock()
	defer master.Unlock()

	workerRPCAddresses := master.workerPoolManager.WorkerRPCAddresses()
	numTasksProcessed := make([]int, 0, len(workerRPCAddresses))
	for _, w := range workerRPCAddresses {
		util.Debug("Master: shutdown worker %s\n", w)
		var reply mr_rpc.ShutdownReply
		ok := mr_rpc.Call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			numTasksProcessed = append(numTasksProcessed, reply.NumTasksProcessed)
		}
	}
	return numTasksProcessed
}
