package workerpool

import "sync"

type WorkerPoolEvent string

const (
	WorkerPoolHasBegunNewWorkSet  = WorkerPoolEvent("WorkerPoolHasBegunNewWorkSet")
	WorkerPoolHasCompletedWorkSet = WorkerPoolEvent("WorkerPoolHasCompletedWorkSet")
	WorkerPoolIsBusy              = WorkerPoolEvent("WorkerPoolIsBusy")
	WorkerPoolIsShuttingDown      = WorkerPoolEvent("WorkerPoolIsShuttingDown")
	WorkerPoolIsShutDown          = WorkerPoolEvent("WorkerPoolIsShutDown")
)

func (workerPool *WorkerPool) triggerEvent(
	workSetIdentifier string,
	event WorkerPoolEvent,
) {
	if listener, ok := workerPool.eventListeners[workSetIdentifier]; ok {
		listener <- event

		if event == WorkerPoolHasCompletedWorkSet {
			close(listener)
			delete(workerPool.eventListeners, workSetIdentifier)
		}
	}
}

func (workerPool *WorkerPool) addEventListener(
	workSetIdentifier string,
	externalChan chan WorkerPoolEvent,
) {
	internalChan := make(chan WorkerPoolEvent)

	go pushEventsToClient(internalChan, externalChan)

	workerPool.eventListeners[workSetIdentifier] = internalChan
}

func pushEventsToClient(
	internalChan chan WorkerPoolEvent,
	externalChan chan WorkerPoolEvent,
) {
	mutex := sync.Mutex{}
	cond := sync.NewCond(&mutex)
	eventsBuffer := make([]WorkerPoolEvent, 0, 16)

	go func() {
		for event := range internalChan {
			func() {
				mutex.Lock()
				defer mutex.Unlock()

				eventsBuffer = append(eventsBuffer, event)

				cond.Signal()
			}()
		}
	}()

	go func() {
		mutex.Lock()
		defer mutex.Unlock()

		for {
			if len(eventsBuffer) > 0 {
				eventsToSend := make([]WorkerPoolEvent, len(eventsBuffer))
				copy(eventsToSend, eventsBuffer)

				jobWasCompleted := func() bool {
					mutex.Unlock()
					defer mutex.Lock()

					for _, event := range eventsToSend {
						externalChan <- event

						if event == WorkerPoolHasCompletedWorkSet {
							return true
						}
					}

					return false
				}()

				if jobWasCompleted {
					close(externalChan)
					break
				}
			}

			cond.Wait()
		}
	}()
}
