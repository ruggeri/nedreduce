package workerpool

type WorkerPoolEvent string

const (
	WorkerPoolCommencedWorkSet    = WorkerPoolEvent("WorkerPoolCommencedWorkSet")
	WorkerPoolDidNotAcceptWorkSet = WorkerPoolEvent("WorkerPoolDidNotAcceptWorkSet")
	WorkerPoolCompletedWorkSet    = WorkerPoolEvent("WorkerPoolCompletedWorkSet")
)
