package workerpool

import (
	"io"
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
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
	if len(tasks) == 0 {
		log.Panic("workSet cannot be created with no tasks to perform.")
	}

	return &workSet{
		isStarted:            false,
		numTasksAssigned:     0,
		numTasksCompleted:    0,
		tasks:                tasks,
		workSetResultChannel: make(chan WorkSetResult),
	}
}

// newWorkSet gets a new task that should be assigned.
func (workSet *workSet) getNextTask() (mr_rpc.Task, error) {
	if !workSet.isStarted {
		log.Panic("workSet can't give out tasks before being explicitly started")
	}

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
