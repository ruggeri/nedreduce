package workerpool

type WorkerPoolEvent string

const (
	WorkerPoolHasBegunNewWorkSet  = WorkerPoolEvent("WorkerPoolHasBegunNewWorkSet")
	WorkerPoolHasCompletedWorkSet = WorkerPoolEvent("WorkerPoolHasCompletedWorkSet")
	WorkerPoolIsBusy              = WorkerPoolEvent("WorkerPoolIsBusy")
	WorkerPoolIsShuttingDown      = WorkerPoolEvent("WorkerPoolIsShuttingDown")
	WorkerPoolIsShutDown          = WorkerPoolEvent("WorkerPoolIsShutDown")
)
