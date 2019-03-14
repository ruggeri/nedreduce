package workerpool

// A Event represents an event about the user's submitted work
// set.
type Event string

const (
	// CommencedWorkSet is fired when the user's work set is commenced.
	CommencedWorkSet = Event("WorkerPoolCommencedWorkSet")

	// DidNotAcceptWorkSet is fired if the user's work set couldn't start
	// because the WorkerPool got shut down.
	DidNotAcceptWorkSet = Event("WorkerPoolDidNotAcceptWorkSet")

	// CompletedWorkSet is fired when the user's work set is completed.
	CompletedWorkSet = Event("WorkerPoolCompletedWorkSet")
)
