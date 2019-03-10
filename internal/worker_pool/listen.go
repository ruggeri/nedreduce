package worker_pool

import (
	"io"
	"log"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/util"
)

func (workerPool *WorkerPool) listenForMessages() {
	for message := range workerPool.messageChannel {
		workerPool.handleMessage(message)
	}
}

func (workerPool *WorkerPool) handleStartingNewWorkSet() {
	for _, workerRPCAddress := range workerPool.workerRPCAddresses {
		if workerPool.state == workingThroughWorkSetTasks {
			workerPool.assignTaskToWorker(workerRPCAddress)
		} else {
			// This can happen if there are more workers than tasks.
			break
		}
	}
}

func (workerPool *WorkerPool) handleTaskCompletion(workerRPCAddress string) {
	util.Debug("worker running at %v finished work assignment\n", workerRPCAddress)
	workerPool.numWorkersWorking--

	if workerPool.state == workingThroughWorkSetTasks {
		workerPool.assignTaskToWorker(workerRPCAddress)
	}

	if workerPool.taskSetIsCompleted() {
		workerPool.handleTaskSetCompletion()
	}
}

func (workerPool *WorkerPool) taskSetIsCompleted() bool {
	if workerPool.state == waitingForLastWorkSetTasksToComplete {
		if workerPool.numWorkersWorking == 0 {
			return true
		}
	}

	return false
}

func (workerPool *WorkerPool) handleTaskSetCompletion() {
	util.Debug("WorkAssigner: all work has been completed\n")

	// Notify whoever schedule this job that we have completed
	go func(workSetResultChannel chan WorkSetResult) {
		workSetResultChannel <- workSetCompleted
	}(workerPool.workSetResultChannel)

	workerPool.state = freeForNewWorkSetToBeAssigned
	workerPool.conditionVariable.Signal()
}

func (workerPool *WorkerPool) handleWorkerRegistration(newWorkerRPCAddress string) {
	util.Debug("worker running at %v entered WorkAssigner pool\n", newWorkerRPCAddress)

	workerPool.workerRPCAddresses = append(
		workerPool.workerRPCAddresses,
		newWorkerRPCAddress,
	)

	if workerPool.state == workingThroughWorkSetTasks {
		workerPool.assignTaskToWorker(newWorkerRPCAddress)
	}
}

func (workerPool *WorkerPool) assignTaskToWorker(workerRPCAddress string) {
	if workerPool.state != workingThroughWorkSetTasks {
		log.Panicf("Shouldn't try to assign work unless there maybe is some to give.")
	}

	nextWorkItem, err := workerPool.workProducingFunction()

	if err == nil {
		util.Debug("WorkAssigner: assigning new work to worker running %v\n", workerRPCAddress)
		workerPool.sendWorkToWorker(nextWorkItem, workerRPCAddress)
		workerPool.numWorkersWorking++
		return
	} else if err != io.EOF {
		log.Panicf("Unexpected work production error: %v\n", err)
	}

	util.Debug("WorkAssigner: all work has been assigned\n")
	workerPool.state = waitingForLastWorkSetTasksToComplete
}

func (workerPool *WorkerPool) handleMessage(message message) {
	switch message.Kind {
	case startNewWorkSetMessage:
		workerPool.handleStartingNewWorkSet()
	case workerRegistrationMessage:
		workerPool.handleWorkerRegistration(message.Address)
	case workerCompletedTaskMessage:
		workerPool.handleTaskCompletion(message.Address)
	}
}

func (workerPool *WorkerPool) sendWorkToWorker(workItem mr_rpc.Task, workerAddress string) {
	workItem.StartOnWorker(workerAddress, func() {
		workerPool.SendWorkerCompletedTaskMessage(workerAddress)
	})
}
