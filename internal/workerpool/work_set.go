package workerpool

import (
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

type taskStatus string

const (
	taskNotStarted = taskStatus("taskNotStarted")
	taskInProgress = taskStatus("taskInProgress")
	taskComplete   = taskStatus("taskComplete")
)

// WorkSetResult represents the result of trying to execute the WorkSet.
// At present, only success is envisioned!
type WorkSetResult string

const (
	workSetCompleted = WorkSetResult("workSetCompleted")
)

// A workSet represents a set of tasks that the WorkerPool should crunch
// through.
type workSet struct {
	// isStarted tells you whether the workSet has been "started." The
	// idea is that no work should be assigned until it has been started.
	isStarted bool

	// tasks are units of work to assign
	tasks map[string]mr_rpc.Task

	taskStatuses map[string]taskStatus

	// workSetResultChannel is used to signal someone when the work is
	// done.
	workSetResultChannel chan WorkSetResult
}

// newWorkSet creates a new workSet.
func newWorkSet(tasks []mr_rpc.Task) *workSet {
	if len(tasks) == 0 {
		log.Panic("workSet cannot be created with no tasks to perform.")
	}

	workSet := &workSet{
		isStarted:            false,
		tasks:                make(map[string]mr_rpc.Task),
		taskStatuses:         make(map[string]taskStatus),
		workSetResultChannel: make(chan WorkSetResult),
	}

	for _, task := range tasks {
		workSet.tasks[task.Identifier()] = task
		workSet.taskStatuses[task.Identifier()] = taskNotStarted
	}

	return workSet
}

func (workSet *workSet) nextUnassignedTask() mr_rpc.Task {
	for taskIdentifier, taskStatus := range workSet.taskStatuses {
		if taskStatus == taskNotStarted {
			return workSet.tasks[taskIdentifier]
		}
	}

	return nil
}

// newWorkSet gets a new task that should be assigned.
func (workSet *workSet) getNextTask() mr_rpc.Task {
	if !workSet.isStarted {
		log.Panic("workSet can't give out tasks before being explicitly started")
	}

	nextTask := workSet.nextUnassignedTask()
	if nextTask == nil {
		return nil
	}

	taskIdentifier := nextTask.Identifier()

	workSet.taskStatuses[taskIdentifier] = taskInProgress
	return nextTask
}

// handleTaskCompletion records that a task has been completed.
func (workSet *workSet) handleTaskCompletion(taskIdentifier string) {
	workSet.taskStatuses[taskIdentifier] = taskComplete

	// If workSet is entirely completed, asynchronously notify whoever is
	// listening.
	if workSet.isCompleted() {
		go func() {
			workSet.workSetResultChannel <- workSetCompleted
		}()
	}
}

// handleTaskFailure records that a task has been failed.
func (workSet *workSet) handleTaskFailure(taskIdentifier string) {
	workSet.taskStatuses[taskIdentifier] = taskNotStarted
}

// isCompleted tells you if all the work in the workSet is completed.
func (workSet *workSet) isCompleted() bool {
	for _, taskStatus := range workSet.taskStatuses {
		if taskStatus != taskComplete {
			return false
		}
	}

	return true
}
