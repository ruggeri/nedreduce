package worker

import (
	"log"
	"time"
)

// TODO: need to, whenever RPC is performed, decrement the number of
// RPCs left until we just shut down the server.

// TODO: this code is unreviewed.

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(f func()) error {
	// fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
	// 	wk.name, arg.JobPhase, arg.TaskIdx, arg.MapInputFileName, arg.NumTasksInOtherPhase)

	wk.mutex.Lock()
	wk.numTasksProcessed += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.mutex.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Panic("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
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

	wk.mutex.Lock()
	wk.concurrent -= 1
	wk.mutex.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.Mu.Lock()
		wk.parallelism.now -= 1
		wk.parallelism.Mu.Unlock()
	}

	// fmt.Printf("%s: %v task #%d done\n", wk.name, arg.JobPhase, arg.TaskIdx)
	return nil
}
