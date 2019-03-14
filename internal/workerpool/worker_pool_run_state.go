package workerpool

type workerPoolRunState string

const (
	workerPoolIsRunning           = workerPoolRunState("workerPoolIsRunning")
	workerPoolShutDownIsRequested = workerPoolRunState("workerPoolShutDownIsRequested")
	workerPoolIsShuttingDown      = workerPoolRunState("workerPoolIsShuttingDown")
	workerPoolIsShutDown          = workerPoolRunState("workerPoolIsShutDown")
)
