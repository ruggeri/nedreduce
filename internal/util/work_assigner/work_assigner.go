package work_assigner

import (
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

type WorkAssigner struct {
	// These are immutable/don't require coordination.
	messageChannel            messageChannel
	workProducingFunction     WorkProducingFunction
	workerRegistrationChannel WorkerRegistrationChannel

	// Use these to coordinate changes to mutable state.
	mutex             sync.Mutex
	conditionVariable *sync.Cond

	// Mutable state
	numWorkersWorking int
	state             state
}

func Start(
	workProducingFunction WorkProducingFunction,
	workerRegistrationChannel WorkerRegistrationChannel,
) *WorkAssigner {
	workAssigner := &WorkAssigner{
		messageChannel:            make(messageChannel),
		workProducingFunction:     workProducingFunction,
		workerRegistrationChannel: workerRegistrationChannel,

		mutex: sync.Mutex{},

		numWorkersWorking: 0,
		state:             assigningNewWork,
	}

	workAssigner.conditionVariable = sync.NewCond(&workAssigner.mutex)

	go workAssigner.listenForMoreWorkers(workerRegistrationChannel)
	go workAssigner.listenForMessages()

	return workAssigner
}

func (workAssigner *WorkAssigner) SendWorkToWorker(workItem mr_rpc.Task, workerAddress string) {
	workItem.StartOnWorker(workerAddress, func() {
		workAssigner.SendWorkCompletedMessage(workerAddress)
	})
}

func (workAssigner *WorkAssigner) SendWorkCompletedMessage(workerAddress string) {
	workAssigner.messageChannel <- message{
		Kind:    workCompletedMessage,
		Address: workerAddress,
	}
}

func (workAssigner *WorkAssigner) Shutdown() {
	close(workAssigner.messageChannel)
	workAssigner.conditionVariable.Broadcast()
}

func (workAssigner *WorkAssigner) Wait() {
	workAssigner.mutex.Lock()
	defer workAssigner.mutex.Unlock()

	for {
		if workAssigner.state == allWorkIsComplete {
			return
		}

		workAssigner.conditionVariable.Wait()
	}
}
