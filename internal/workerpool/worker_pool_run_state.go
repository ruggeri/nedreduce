package workerpool

type workerPoolRunState string

const (
	workerPoolIsRunning      = workerPoolRunState("workerPoolIsRunning")
	workerPoolIsShuttingDown = workerPoolRunState("workerPoolIsShuttingDown")
	workerPoolIsShutDown     = workerPoolRunState("workerPoolIsShutDown")
)
