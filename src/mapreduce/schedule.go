package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		runMapPhase(
			registerChan,
			jobName,
			mapFiles,
			nReduce,
		)
	case reducePhase:
		runReducePhase(
			registerChan,
			jobName,
			len(mapFiles),
			nReduce,
		)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
}

func runMapPhase(registerChan chan string, jobName string, mapFiles []string, numReducers int) {
	wg := &sync.WaitGroup{}
	workChannel := make(chan DoTaskArgs)
	allWorkScheduled := make(chan struct{})

	// Someone should be pushing in work to the channel.
	wg.Add(1)
	go pushMapWork(wg, workChannel, allWorkScheduled, jobName, mapFiles, numReducers)

	// Run map tasks on available workers.
	go func() {
		for {
			select {
			case workerRPCAddress := <-registerChan:
				wg.Add(1)
				go runWorker(wg, workerRPCAddress, workChannel)
			case <-allWorkScheduled:
				break
			}
		}
	}()

	wg.Wait()
}

func runReducePhase(registerChan chan string, jobName string, numMappers int, numReducers int) {
	wg := &sync.WaitGroup{}
	workChannel := make(chan DoTaskArgs)
	allWorkScheduled := make(chan struct{})

	// Someone should be pushing in work to the channel.
	wg.Add(1)
	go pushReduceWork(wg, workChannel, allWorkScheduled, jobName, numMappers, numReducers)

	// Run reduce tasks on available workers.
	go func() {
		for {
			select {
			case workerRPCAddress := <-registerChan:
				wg.Add(1)
				go runWorker(wg, workerRPCAddress, workChannel)
			case <-allWorkScheduled:
				break
			}
		}
	}()

	wg.Wait()
}

func pushMapWork(wg *sync.WaitGroup, workChannel chan DoTaskArgs, allWorkScheduled chan struct{}, jobName string, mapFiles []string, numReducers int) {
	for mapTaskIdx := 0; mapTaskIdx < len(mapFiles); mapTaskIdx++ {
		args := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[mapTaskIdx],
			Phase:         mapPhase,
			TaskNumber:    mapTaskIdx,
			NumOtherPhase: numReducers,
		}

		workChannel <- args
	}

	close(workChannel)
	allWorkScheduled <- struct{}{}
	wg.Done()
}

func pushReduceWork(wg *sync.WaitGroup, workChannel chan DoTaskArgs, allWorkScheduled chan struct{}, jobName string, numMappers int, numReducers int) {
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		args := DoTaskArgs{
			JobName:       jobName,
			Phase:         reducePhase,
			TaskNumber:    reduceTaskIdx,
			NumOtherPhase: numMappers,
		}

		workChannel <- args
	}

	close(workChannel)
	allWorkScheduled <- struct{}{}
	wg.Done()
}

func runWorker(wg *sync.WaitGroup, workerRPCAddress string, workChannel chan DoTaskArgs) {
	for doTaskArgs := range workChannel {
		call(workerRPCAddress, "Worker.DoTask", doTaskArgs, nil)
	}

	wg.Done()
}
