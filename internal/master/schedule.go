package master

import (
	"fmt"
	"log"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	. "github.com/ruggeri/nedreduce/pkg/types"
)

// A WorkerRegistrationChannel is a channel on which the master can push
// the addresses of workers as they register in real time.
type WorkerRegistrationChannel chan string

// runDistributedPhase is a generic function which works for either the
// MapPhase or the ReducePhase. It calls _runDistributedPhase, simply
// varying the function which pushes map work.
func runDistributedPhase(
	jobConfiguration *JobConfiguration,
	jobPhase JobPhase,
	registerChan chan string,
) {
	numMappers := jobConfiguration.NumMappers()
	numReducers := jobConfiguration.NumReducers

	switch jobPhase {
	case MapPhase:
		fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", numMappers, jobPhase, numReducers)

		_runDistributedPhase(
			registerChan,
			func(wg *sync.WaitGroup, workChannel WorkChannel, noMoreWorkChannel NoMoreWorkChannel) {
				pushMapWork(wg, workChannel, noMoreWorkChannel, jobConfiguration)
			},
		)
	case ReducePhase:
		fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", numReducers, jobPhase, numMappers)

		_runDistributedPhase(
			registerChan,
			func(wg *sync.WaitGroup, workChannel WorkChannel, noMoreWorkChannel NoMoreWorkChannel) {
				pushReduceWork(wg, workChannel, noMoreWorkChannel, jobConfiguration)
			},
		)
	}
}

// A WorkChannel is a channel by which a WorkSchedulingFunction pushes
// work to Workers.
type WorkChannel chan mr_rpc.DoTaskArgs

// A NoMoreWorkChannel is a channel by which a WorkSchedulingFunction
// tells a listener for newly registered Workers that there will be no
// more work.
type NoMoreWorkChannel chan struct{}

// A WorkSchedulingFunction is a function that will push down work to
// Workers over the WorkChannel. The WorkSchedulingFunction must call
// wg.Done() to let the caller know when it is complete.
type WorkSchedulingFunction func(
	wg *sync.WaitGroup,
	workChannel WorkChannel,
	noMoreWorkChannel NoMoreWorkChannel,
)

// _runDistributedPhase is where the magic happens. It is here where we
// fork a goroutine to learn about available workers. Each time we learn
// about a new worker, we use it by forking another goroutine to call
// runWorker.
func _runDistributedPhase(
	workerRegistrationChannel WorkerRegistrationChannel,
	workSchedulingFunction WorkSchedulingFunction,
) {
	wg := &sync.WaitGroup{}
	workChannel := make(WorkChannel)
	allWorkScheduled := make(NoMoreWorkChannel)

	// workSchedulingFunction will push down work on the workChannel.
	wg.Add(1)
	go workSchedulingFunction(wg, workChannel, allWorkScheduled)

	// Listens for available workers and starts using them until no more
	// work remains.
	go func() {
		for {
			select {
			case <-allWorkScheduled:
				// Stop listening for new workers if there won't be any more
				// work.
				break
			case workerRPCAddress := <-workerRegistrationChannel:
				// As we learn about new workers, start running work on them.
				wg.Add(1)
				go runWorker(wg, workerRPCAddress, workChannel)
			}
		}
	}()

	// Wait until all work is scheduled and all workers are done.
	wg.Wait()
}

// pushMapWork just keeps pushing down mapper work on workChannel until
// there are no more map tasks.
func pushMapWork(
	wg *sync.WaitGroup,
	workChannel WorkChannel,
	noMoreWorkChannel NoMoreWorkChannel,
	jobConfiguration *JobConfiguration,
) {
	jobName := jobConfiguration.JobName
	numMappers := jobConfiguration.NumMappers()
	numReducers := jobConfiguration.NumReducers

	for mapTaskIdx := 0; mapTaskIdx < numMappers; mapTaskIdx++ {
		mapInputFileName := jobConfiguration.MapperInputFileNames[mapTaskIdx]
		args := mr_rpc.DoTaskArgs{
			JobName:              jobName,
			JobPhase:             MapPhase,
			MapInputFileName:     mapInputFileName,
			TaskIdx:              mapTaskIdx,
			NumTasksInOtherPhase: numReducers,
		}

		workChannel <- args
	}

	// Close channel so that current workers stop listening for more work.
	close(workChannel)
	// Send so that listener for new workers can stop listening (since
	// there's no more work).
	noMoreWorkChannel <- struct{}{}
	// Let caller know we're done scheduling work.
	wg.Done()
}

// pushReduceWork just keeps pushing down reducer work on workChannel
// until there are no more reduce tasks.
func pushReduceWork(
	wg *sync.WaitGroup,
	workChannel WorkChannel,
	noMoreWorkChannel NoMoreWorkChannel,
	jobConfiguration *JobConfiguration,
) {
	jobName := jobConfiguration.JobName
	numMappers := jobConfiguration.NumMappers()
	numReducers := jobConfiguration.NumReducers

	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		args := mr_rpc.DoTaskArgs{
			JobName:              jobName,
			JobPhase:             ReducePhase,
			TaskIdx:              reduceTaskIdx,
			NumTasksInOtherPhase: numMappers,
		}

		workChannel <- args
	}

	// Close channel so that current workers stop listening for more work.
	close(workChannel)
	// Send so that listener for new workers can stop listening (since
	// there's no more work).
	noMoreWorkChannel <- struct{}{}
	// Let caller know we're done scheduling work.
	wg.Done()
}

// runWorker keeps pulling down work from workChannel and instructs the
// remote worker to execute the task.
func runWorker(
	wg *sync.WaitGroup,
	workerRPCAddress string,
	workChannel WorkChannel,
) {
	for doTaskArgs := range workChannel {
		// For each piece of work we can claim, we will run it remotely on
		// the worker.
		ok := mr_rpc.Call(workerRPCAddress, "Worker.DoTask", doTaskArgs, nil)

		if !ok {
			log.Fatal("Something went wrong with RPC call to worker.")
		}
	}

	// Let the caller know this worker has completed all its alloted work.
	wg.Done()
}
