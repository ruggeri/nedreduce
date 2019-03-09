package work_assigner

// A WorkItem represents a unit of work to be performed by a worker.
type WorkItem interface {
	StartOnWorker(workerAddress string, workCompletionCallback WorkCompletionCallback)
}

// A WorkProducingFunction is how a user of the WorkAssigner produces
// WorkItems to be assigned. When there is no more work to be assigned,
// the WorkProducingFunction should return nil.
type WorkProducingFunction func() WorkItem

type WorkCompletionCallback func()
