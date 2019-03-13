package workerpool

type workerPoolRunState string

const (
	freeForWorkSet   = workerPoolRunState("freeForWorkSet")
	workingOnWorkSet = workerPoolRunState("workingOnWorkSet")
	shuttingDown     = workerPoolRunState("shuttingDown")
	shutDown         = workerPoolRunState("shutDown")
)
