package worker

type WorkerEvent string

const (
	rpcReceived = WorkerEvent("rpcReceived")
	taskStart   = WorkerEvent("taskStart")
	taskEnd     = WorkerEvent("taskEnd")
)

type WorkerAction string

const (
	doNothing = WorkerAction("doNothing")
	failRPC   = WorkerAction("failRPC")
)

type EventListener interface {
	OnWorkerEvent(worker *Worker, workerEvent WorkerEvent) WorkerAction
}
