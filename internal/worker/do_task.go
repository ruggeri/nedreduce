package worker

import (
	"errors"
	"log"
)

// DoTask performs the taskFunc is provided. This is how the Worker
// executes the JobCoordinator's task.
func (worker *Worker) DoTask(taskFunc func()) error {
	// First check that we are allowed to run this new task. If so, then
	// record that we are currently running a job.
	err := worker.checkAndUpdateRunStateBeforeNextTask()

	if err != nil {
		return err
	}

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

	return nil
}

// checkAndUpdateRunStateBeforeNextTask checks that we are properly
// ready to run a job.
func (worker *Worker) checkAndUpdateRunStateBeforeNextTask() error {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	switch worker.runState {
	case availableForNextTask:
		// Good to go!
	case runningATask:
		// Worker rejects work if it is already working. It's the
		// JobCoordinator's responsibility not to give us two tasks at once.
		// There shouldn't be two coordinators out there to give us two
		// conflicting tasks. They have to figure that out.
		return errors.New("WorkerIsAlreadyWorkingOnATask")
	case shutDown:
		// Worker rejects work if it is shut down (duh). Again,
		// JobCoordinator should figure out what to do with this error.
		return errors.New("WorkerIsShutDown")
	default:
		log.Panicf(
			"non-exhaustive worker runState switch: %v\n",
			worker.runState,
		)
	}

	worker.numTasksProcessed++
	worker.runState = runningATask
	return nil
}

// restoreRunStateAfterTaskCompletion restores the Worker's runState so
// that it can accept new jobs.
func (worker *Worker) restoreRunStateAfterTaskCompletion() {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	// Switch here is a sanity check.
	switch worker.runState {
	case runningATask:
		worker.runState = availableForNextTask
	default:
		log.Panicf("worker state changed while running job??")
	}

	worker.runStateCond.Broadcast()
}
