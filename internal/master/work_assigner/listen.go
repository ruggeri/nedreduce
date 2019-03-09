package work_assigner

import "github.com/ruggeri/nedreduce/internal/util"

type WorkerRegistrationChannel chan string

func (workAssigner *WorkAssigner) listenForMoreWorkers(
	workerRegistrationChannel WorkerRegistrationChannel,
) {
	for newWorkerAddress := range workerRegistrationChannel {
		workAssigner.messageChannel <- message{
			Kind:    workerRegistrationMessage,
			Address: newWorkerAddress,
		}
	}
}

func (workAssigner *WorkAssigner) listenForMessages() {
	for message := range workAssigner.messageChannel {
		workAssigner.handleMessage(message)
	}
}

func (workAssigner *WorkAssigner) handleMessage(message message) {
	workAssigner.mutex.Lock()
	defer workAssigner.mutex.Unlock()

	if message.Kind == workerRegistrationMessage {
		util.Debug("worker running at %v entered WorkAssigner pool", message.Address)
		workAssigner.numWorkersWorking++
	} else {
		util.Debug("worker running at %v finished work assignment", message.Address)
		workAssigner.numWorkersWorking--
	}

	nextWorkItem := workAssigner.workProducingFunction()

	if nextWorkItem != nil {
		util.Debug("WorkAssigner: assigning new work to worker running %v", message.Address)
		workAssigner.SendWorkToWorker(nextWorkItem, message.Address)
		workAssigner.numWorkersWorking++
		return
	}

	if workAssigner.state == assigningNewWork {
		util.Debug("WorkAssigner: all work has been assigned")
		workAssigner.state = waitingForLastWorksToComplete
	}

	if workAssigner.numWorkersWorking == 0 {
		util.Debug("WorkAssigner: all work has been completed")
		workAssigner.state = allWorkIsComplete
		workAssigner.Shutdown()
	}
}
