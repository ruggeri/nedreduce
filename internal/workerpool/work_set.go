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

// A workSet represents a set of tasks that the WorkerPool should crunch
// through.
type workSet struct {
	// Tasks are the units of work to assign.
	tasks map[string]mr_rpc.Task
	// Each task has a status.
	taskStatuses map[string]taskStatus
}

// newWorkSet creates a new workSet.
func newWorkSet(tasks []mr_rpc.Task) *workSet {
	if len(tasks) == 0 {
		log.Panic("workSet cannot be created with no tasks to perform.")
	}

	workSet := &workSet{
		tasks:        make(map[string]mr_rpc.Task),
		taskStatuses: make(map[string]taskStatus),
	}

	for _, task := range tasks {
		workSet.tasks[task.Identifier()] = task
		workSet.taskStatuses[task.Identifier()] = taskNotStarted
	}

	return workSet
}

func (workSet *workSet) takeFirstUnassignedTask() mr_rpc.Task {
	var nextTask mr_rpc.Task
	for taskIdentifier, taskStatus := range workSet.taskStatuses {
		if taskStatus == taskNotStarted {
			nextTask = workSet.tasks[taskIdentifier]
		}
	}

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
}

// handleTaskFailure returns a task to the work set.
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
