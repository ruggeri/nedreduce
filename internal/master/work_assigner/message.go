package work_assigner

type messageKind string

const (
	workerRegistrationMessage = messageKind("workerRegistrationMessage")
	workCompletedMessage      = messageKind("workCompletedMessage")
)

type message struct {
	Kind    messageKind
	Address string
}

// A messageChannel is a channel we can push workerMessages over.
// This is used by the WorkerPoolManager to notify us of newly
// registered workers, and it is also used when a worker has completed
// some work and wants to be assigned a new task.
type messageChannel chan message
