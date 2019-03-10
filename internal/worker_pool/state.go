package worker_pool

type state string

const (
	freeForNewWorkSetToBeAssigned        = state("freeForNewWorkSetToBeAssigned")
	workingThroughWorkSetTasks           = state("workingThroughWorkSetTasks")
	waitingForLastWorkSetTasksToComplete = state("waitingForLastWorkSetTasksToComplete")
)
