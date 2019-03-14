package workerpool

// TODO: Investigate this code?

// func pushEventsToClient(
// 	internalChan chan WorkerPoolEvent,
// 	externalChan chan WorkerPoolEvent,
// ) {
// 	mutex := sync.Mutex{}
// 	cond := sync.NewCond(&mutex)
// 	eventsBuffer := make([]WorkerPoolEvent, 0, 16)

// 	go func() {
// 		for event := range internalChan {
// 			func() {
// 				mutex.Lock()
// 				defer mutex.Unlock()

// 				eventsBuffer = append(eventsBuffer, event)

// 				cond.Signal()
// 			}()
// 		}
// 	}()

// 	go func() {
// 		mutex.Lock()
// 		defer mutex.Unlock()

// 		for {
// 			if len(eventsBuffer) > 0 {
// 				eventsToSend := make([]WorkerPoolEvent, len(eventsBuffer))
// 				copy(eventsToSend, eventsBuffer)

// 				jobWasCompleted := func() bool {
// 					mutex.Unlock()
// 					defer mutex.Lock()

// 					for _, event := range eventsToSend {
// 						externalChan <- event

// 						if event == WorkerPoolHasCompletedWorkSet {
// 							return true
// 						}
// 					}

// 					return false
// 				}()

// 				if jobWasCompleted {
// 					close(externalChan)
// 					break
// 				}
// 			}

// 			cond.Wait()
// 		}
// 	}()
// }
