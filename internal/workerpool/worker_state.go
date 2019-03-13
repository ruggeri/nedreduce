package workerpool

type workerState string

const (
	failed        = workerState("failed")
	freeForTask   = workerState("availableForTask")
	workingOnTask = workerState("workingOnTask")
)
