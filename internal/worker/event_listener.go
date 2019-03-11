package worker

// Event is the name of an event that an EventListener can listen
// for.
type Event string

const (
	rpcReceived = Event("rpcReceived")
	taskStart   = Event("taskStart")
	taskEnd     = Event("taskEnd")
)

// A Action is an action that an EventListener can ask to be
// performed.
type Action string

const (
	doNothing = Action("doNothing")
	failRPC   = Action("failRPC")
)

// An EventListener can listen for events from the worker, so that it
// can test internal behavior. The listener can also inject behavior
// into the Worker via Actions.
type EventListener interface {
	// OnWorkerEvent is called by the Worker to hand off Events to the
	// listener. The Listener can return an Action to perform.
	OnWorkerEvent(worker *Worker, workerEvent Event) Action
}
