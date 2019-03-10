package worker_pool

import (
	"io"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

type WorkSetResult string

const (
	workSetCompleted = WorkSetResult("workSetCompleted")
)

// A workSet represents a set of tasks that the WorkerPool should crunch
// through.
type workSet struct {
	// numTasksAssigned is the number of tasks that are currently assigned
	// to a worker.
	numTasksAssigned int

	// numTasksCompleted is the number of tasks that have been completed
	numTasksCompleted int

	// tasks are units of work to assign
	tasks []mr_rpc.Task

	// workSetResultChannel is used to signal someone when the work is
	// done.
	workSetResultChannel chan WorkSetResult
}

// newWorkSet creates a new workSet.
func newWorkSet(tasks []mr_rpc.Task) *workSet {
	return &workSet{
		numTasksAssigned:     0,
		numTasksCompleted:    0,
		tasks:                tasks,
		workSetResultChannel: make(chan WorkSetResult),
	}
}

// newWorkSet gets a new task that should be assigned.
func (workSet *workSet) getNextTask() (mr_rpc.Task, error) {
	currentTaskIdx := workSet.numTasksCompleted + workSet.numTasksAssigned

	if currentTaskIdx == len(workSet.tasks) {
		return nil, io.EOF
	}

	nextTask := workSet.tasks[currentTaskIdx]
	workSet.numTasksAssigned++
	return nextTask, nil
}

// handleTaskCompletion records that a task has been completed.
func (workSet *workSet) handleTaskCompletion() {
	workSet.numTasksCompleted++
	workSet.numTasksAssigned--

	// If workSet is entirely completed, asynchronously notify whoever is
	// listening.
	if workSet.isCompleted() {
		go func() {
			workSet.workSetResultChannel <- workSetCompleted
		}()
	}
}

// isCompleted tells you if all the work in the workSet is completed.
func (workSet *workSet) isCompleted() bool {
	return workSet.numTasksCompleted == len(workSet.tasks)
}
