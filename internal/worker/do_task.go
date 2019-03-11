package worker

import (
	"log"
)

// TODO: need to, whenever RPC is performed, decrement the number of
// RPCs left until we just shut down the server.

// TODO: this code is unreviewed.

// DoTask performs the taskFunc is provided. This is how the Worker
// executes the JobCoordinator's task.
func (worker *Worker) DoTask(taskFunc func()) {
	// First check that we are allowed to run this new task. If so, then
	// record that we are currently running a job.
	worker.checkAndUpdateRunStateBeforeNextTask()

	if worker.parallelism != nil {
		worker.parallelism.OnTaskStart(worker)
	}

	// Perform the task.
	taskFunc()

	// Restore the runState so that new jobs can be accepted.
	worker.restoreRunStateAfterTaskCompletion()

	if worker.parallelism != nil {
		worker.parallelism.OnTaskEnd(worker)
	}
}

// checkAndUpdateRunStateBeforeNextTask checks that we aren't already
// running a job, and updates our runState to reflect that we are.
func (worker *Worker) checkAndUpdateRunStateBeforeNextTask() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	switch worker.runState {
	case availableForNextJob:
		worker.numTasksProcessed++
		worker.runState = runningJob
		return
	case runningJob:
		log.Panicf(
			"worker @ %v assigned more than one task at a time.\n",
			worker.rpcAddress,
		)
	case shutDown:
		log.Panicf(
			"worker @ %v assigned a task after shut down.\n",
			worker.rpcAddress,
		)
	default:
		log.Panicf(
			"non-exhaustive worker runState switch: %v\n",
			worker.runState,
		)
	}
}

// restoreRunStateAfterTaskCompletion restores the Worker's runState so
// that it can accept new jobs.
func (worker *Worker) restoreRunStateAfterTaskCompletion() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	switch worker.runState {
	case runningJob:
		worker.runState = availableForNextJob
	default:
		log.Panicf("worker state changed while running job??")
	}
}
