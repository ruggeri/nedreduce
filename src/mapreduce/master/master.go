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
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName string   // Name of currently executing job
	Files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	Stats    []int
}

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (mr *Master) Register(args *mr_rpc.RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	common.Debug("Register: worker %s\n", args.WorkerRPCAdress)
	mr.workers = append(mr.workers, args.WorkerRPCAdress)

	// tell forwardRegistrations() that there's a new workers[] entry.
	mr.newCond.Broadcast()

	return nil
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.Address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.DoneChannel = make(chan bool)
	return
}

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before running the next.
func Sequential(jobName string, files []string, nreduce int,
	mappingFunction mapper.MappingFunction,
	reducingFunction reducer.ReducingFunction,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase common.JobPhase) {
		switch phase {
		case common.MapPhase:
			for i, f := range mr.Files {
				mapper.ExecuteMapping(mr.jobName, i, f, mr.nReduce, mappingFunction)
			}
		case common.ReducePhase:
			for i := 0; i < mr.nReduce; i++ {
				reducer.ExecuteReducing(mr.jobName, i, common.OutputFileName(mr.jobName, i), len(mr.Files), reducingFunction)
			}
		}
	}, func() {
		mr.Stats = []int{len(files) + nreduce}
	})
	return
}

// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := mr.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Distributed schedules map and reduce tasks on workers that register with the
// master over RPC.
func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce,
		func(phase common.JobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.Files, mr.nReduce, phase, ch)
		},
		func() {
			mr.Stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase common.JobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.Files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.Address, mr.jobName)

	schedule(common.MapPhase)
	schedule(common.ReducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.Address)

	mr.DoneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.DoneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		common.Debug("Master: shutdown worker %s\n", w)
		var reply mr_rpc.ShutdownReply
		ok := mr_rpc.Call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.NumTasksProcessed)
		}
	}
	return ntasks
}
