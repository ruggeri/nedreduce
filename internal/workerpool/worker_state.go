package workerpool

// workerState tracks whether a worker is free for a new task, already
// working on one, or (god forbid) failed. :-)
type workerState string

const (
	failed        = workerState("failed")
	freeForTask   = workerState("availableForTask")
	workingOnTask = workerState("workingOnTask")
)
