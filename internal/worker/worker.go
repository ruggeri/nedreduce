package worker

//
// Please do not modify this file.
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/ruggeri/nedreduce/internal/mapper"
	"github.com/ruggeri/nedreduce/internal/reducer"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

// track whether workers executed in parallel.
type Parallelism struct {
	Mu  sync.Mutex
	now int32
	Max int32
}

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name        string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	l           net.Listener
	parallelism *Parallelism
}

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

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(f func()) error {
	// fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
	// 	wk.name, arg.JobPhase, arg.TaskIdx, arg.MapInputFileName, arg.NumTasksInOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.Mu.Lock()
		wk.parallelism.now += 1
		if wk.parallelism.now > wk.parallelism.Max {
			wk.parallelism.Max = wk.parallelism.now
		}
		if wk.parallelism.Max < 2 {
			pause = true
		}
		wk.parallelism.Mu.Unlock()
	}

	if pause {
		// give other workers a chance to prove that
		// they are executing in parallel.
		time.Sleep(time.Second)
	}

	f()

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.Mu.Lock()
		wk.parallelism.now -= 1
		wk.parallelism.Mu.Unlock()
	}

	// fmt.Printf("%s: %v task #%d done\n", wk.name, arg.JobPhase, arg.TaskIdx)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *mr_rpc.WorkerShutdownResponse) error {
	util.Debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.NumTasksProcessed = wk.nTasks
	wk.nRPC = 1
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(mr_rpc.WorkerRegistrationMessage)
	args.WorkerRPCAdress = wk.name
	ok := mr_rpc.Call(master, "Master.RegisterWorker", args, new(struct{}))
	if ok == false {
		fmt.Printf("RegisterWorker: RPC %s register error\n", master)
	}
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(MasterAddress string, me string,
	nRPC int, parallelism *Parallelism,
) {
	util.Debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.nRPC = nRPC
	wk.parallelism = parallelism
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	util.Debug("RunWorker %s exit\n", me)
}
