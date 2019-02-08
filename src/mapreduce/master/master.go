package master

//
// Please do not modify this file.
//

import (
	"fmt"
	"mapreduce/common"
	"mapreduce/mapper"
	"mapreduce/reducer"
	mr_rpc "mapreduce/rpc"
	"net"
	"sync"
)

// Master holds all the state that the master needs to keep track of.
type Master struct {
	sync.Mutex

	Address     string
	DoneChannel chan bool

	// protected by the mutex
	newWorkerConditionVariable *sync.Cond // signals when Register() adds to workers[]
	workers                    []string   // each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName              string   // Name of currently executing job
	MapperInputFileNames []string // Input files
	numReducers          int      // Number of reduce partitions

	shutdown           chan struct{}
	connectionListener net.Listener
	Stats              []int
}

// newMaster initializes a new Map/Reduce Master
func newMaster(
	masterAddress string,
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
) (master *Master) {
	master = new(Master)
	master.Address = masterAddress
	master.shutdown = make(chan struct{})
	master.newWorkerConditionVariable = sync.NewCond(master)
	master.DoneChannel = make(chan bool)

	master.jobName = jobName
	master.MapperInputFileNames = mapperInputFileNames
	master.numReducers = numReducers
	return
}

// RunSequentialJob runs map and reduce tasks sequentially, waiting for
// each task to complete before running the next.
func RunSequentialJob(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	mappingFunction mapper.MappingFunction,
	reducingFunction reducer.ReducingFunction,
) (master *Master) {
	master = newMaster("master", jobName, mapperInputFileNames, numReducers)

	// Master's coordination/execution of the MapReduce job will run in a
	// background thread.
	go master.runJob(
		jobName,
		mapperInputFileNames,
		numReducers,
		// This function executes each of the two phases.
		func(jobPhase common.JobPhase) {
			switch jobPhase {
			case common.MapPhase:
				// Run each map task one-by-one.
				for mapTaskIdx, mapperInputFileName := range master.MapperInputFileNames {
					mapper.ExecuteMapping(jobName, mapTaskIdx, mapperInputFileName, numReducers, mappingFunction)
				}
			case common.ReducePhase:
				// Run each reduce task one-by-one.
				numMappers := len(mapperInputFileNames)
				for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
					reducer.ExecuteReducing(jobName, reduceTaskIdx, numMappers, reducingFunction)
				}
			}
		},
		// This function collects stats when both phases are complete.
		func() {
			master.Stats = []int{len(mapperInputFileNames) + numReducers}
		},
	)

	return
}

// RunDistributedJob schedules map and reduce tasks on workers that
// register with the master over RPC.
func RunDistributedJob(
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	masterAddress string,
) (master *Master) {
	// First construct the Master and start running an RPC server which
	// can listen for connections.
	master = newMaster("master", jobName, mapperInputFileNames, numReducers)
	master.startRPCServer()

	go master.runJob(
		jobName,
		mapperInputFileNames,
		numReducers,
		// This function is used to execute each job phase.
		func(jobPhase common.JobPhase) {
			// Start running someone to listen for workers to register with
			// the master. As workers register, we will add them to our pool
			// of available workers.
			workerRegistrationChannel := make(WorkerRegistrationChannel)
			go master.forwardWorkerRegistrations(workerRegistrationChannel)

			runDistributedPhase(
				jobName,
				mapperInputFileNames,
				numReducers,
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
	jobName string,
	mapperInputFileNames []string,
	numReducers int,
	runPhase func(phase common.JobPhase),
	collectStatsAndCleanup func(),
) {
	fmt.Printf("%s: Starting Map/Reduce task %s\n", master.Address, master.jobName)

	runPhase(common.MapPhase)
	runPhase(common.ReducePhase)
	collectStatsAndCleanup()
	common.MergeReducerOutputFiles(jobName, numReducers)

	fmt.Printf("%s: Map/Reduce task completed\n", master.Address)

	master.DoneChannel <- true
}

// forwardWorkerRegistrations sends all registered workers to the phase,
// and then forwards more workers as they connect.
func (master *Master) forwardWorkerRegistrations(
	workerRegistrationChannel WorkerRegistrationChannel,
) {
	i := 0
	for {
		// We lock to synchronize access to the slice of workers.
		master.Lock()
		if len(master.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := master.workers[i]
			go func() { workerRegistrationChannel <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for RegisterWorker to add an entry to workers[] in
			// response to a registration RPC from a new worker.
			master.newWorkerConditionVariable.Wait()
		}
		master.Unlock()
	}
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has
// performed.
func (master *Master) killWorkers() []int {
	master.Lock()
	defer master.Unlock()

	numTasksProcessed := make([]int, 0, len(master.workers))
	for _, w := range master.workers {
		common.Debug("Master: shutdown worker %s\n", w)
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
