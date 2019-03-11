package worker

type WorkerEvent string

const (
	taskStart = WorkerEvent("taskStart")
	taskEnd   = WorkerEvent("taskEnd")
)

type WorkerAction string

const (
	doNothing  = WorkerAction("doNothing")
	killWorker = WorkerAction("killWorker")
)

type EventListener interface {
	OnWorkerEvent(worker *Worker, workerEvent WorkerEvent) WorkerAction
}
