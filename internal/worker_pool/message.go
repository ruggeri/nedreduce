package worker_pool

type messageKind string

const (
	startNewWorkSetMessage     = messageKind("startNewWorkSetMessage")
	workerRegistrationMessage  = messageKind("workerRegistrationMessage")
	workerCompletedTaskMessage = messageKind("workerCompletedTaskMessage")
)

type message struct {
	Kind    messageKind
	Address string
}

// A messageChannel is a channel we can push internal messages over.
// This is used by the WorkerPool to notify us of newly registered
// workers, and it is also used when a worker has completed some work
// and wants to be assigned a new task.
type messageChannel chan message
