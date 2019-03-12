package worker

import (
	"log"
)

// DoTask performs the taskFunc is provided. This is how the Worker
// executes the JobCoordinator's task.
func (worker *Worker) DoTask(taskFunc func()) {
	// First check that we are allowed to run this new task. If so, then
	// record that we are currently running a job.
	worker.checkAndUpdateRunStateBeforeNextTask()

	for _, eventListener := range worker.eventListeners {
		eventListener.OnWorkerEvent(worker, taskStart)
	}

	// Perform the task.
	taskFunc()

	// Restore the runState so that new jobs can be accepted.
	worker.restoreRunStateAfterTaskCompletion()

	for _, eventListener := range worker.eventListeners {
		eventListener.OnWorkerEvent(worker, taskEnd)
	}
}

// checkAndUpdateRunStateBeforeNextTask checks that we are properly
// ready to run a job.
func (worker *Worker) checkAndUpdateRunStateBeforeNextTask() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	switch worker.runState {
	case availableForNextTask:
		worker.numTasksProcessed++
		worker.runState = runningATask
		return
	case runningATask:
		// TODO(MEDIUM): don't panic. Just refuse the job.
		log.Panicf(
			"worker @ %v assigned more than one task at a time.\n",
			worker.rpcAddress,
		)
	case shutDown:
		// TODO(MEDIUM): don't panic. Just refuse the job.
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
	case runningATask:
		worker.runState = availableForNextTask
	default:
		log.Panicf("worker state changed while running job??")
	}

	worker.runStateCond.Broadcast()
}
