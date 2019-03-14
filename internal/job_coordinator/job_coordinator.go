package job_coordinator

import (
	"errors"
	"sync"

	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/workerpool"
)

// runState can be either "running" or "shutDown"
type runState string

const (
	readyForNewJob = runState("readyForNewJob")
	runningAJob    = runState("runningAJob")
	shutDown       = runState("shutDown")
)

// JobCoordinator holds all the state that the jobCoordinator needs to
// keep track of.
type JobCoordinator struct {
	mutex        sync.Mutex
	runStateCond *sync.Cond

	address        string
	currentJobName *string
	jobStatuses    map[string]bool
	rpcServer      *mr_rpc.Server
	workerPool     *workerpool.WorkerPool
	runState       runState
}

// StartJobCoordinator creates a new JobCoordinator and starts it
// running an RPC Server and a WorkerPool.
func StartJobCoordinator(
	jobCoordinatorAddress string,
) *JobCoordinator {
	jobCoordinator := &JobCoordinator{
		address:     jobCoordinatorAddress,
		jobStatuses: make(map[string]bool),
		rpcServer:   nil,
		workerPool:  workerpool.Start(),
		runState:    readyForNewJob,
	}

	jobCoordinator.runStateCond = sync.NewCond(&jobCoordinator.mutex)
	jobCoordinator.rpcServer = startJobCoordinatorRPCServer(jobCoordinator)

	return jobCoordinator
}

// Address is merely a getter used elsewhere (simply for logging, I
// think).
func (jobCoordinator *JobCoordinator) Address() string {
	return jobCoordinator.address
}

// markJobAsCompleted tells the jobCoordinator that a job is complete.
func (jobCoordinator *JobCoordinator) markJobAsCompleted() {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	currentJobName := *jobCoordinator.currentJobName
	jobCoordinator.jobStatuses[currentJobName] = true
	jobCoordinator.currentJobName = nil
	jobCoordinator.runState = readyForNewJob

	// And last, let waiters know that a job is complete.
	jobCoordinator.runStateCond.Broadcast()
}

// StartJob asks the JobCoordinator to start the specified job. It will
// refuse execution if currently executing another job.
func (jobCoordinator *JobCoordinator) StartJob(
	jobConfiguration *types.JobConfiguration,
) error {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	// TODO(LOW): it's reasonable that two users of the NedReduce cluster
	// should be able to submit jobs at the same time. They should have
	// the option of queueing their jobs.
	switch jobCoordinator.runState {
	case readyForNewJob:
		// Good to go!
	case runningAJob:
		return errors.New("CoordinatorIsAlreadyWorkingOnAJob")
	case shutDown:
		return errors.New("CoordinatorIsShutDown")
	}

	jobCoordinator.currentJobName = &jobConfiguration.JobName
	jobCoordinator.runState = runningAJob
	go jobCoordinator.executeJob(jobConfiguration)

	jobCoordinator.runStateCond.Broadcast()

	return nil
}

// Shutdown tells the JobCoordinator to shut down. The JobCoordinator
// will wait until any currently running jobs are completed.
func (jobCoordinator *JobCoordinator) Shutdown() {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	for {
		if jobCoordinator.runState == shutDown {
			// Someone else has shut us down.
			return
		} else if jobCoordinator.runState == readyForNewJob {
			// No one is running; we can now shut down!
			break
		}

		jobCoordinator.runStateCond.Wait()
	}

	// Tell the RPC server and the workerRegistrationManager to both shut
	// themselves down.
	jobCoordinator.rpcServer.Shutdown()
	jobCoordinator.workerPool.Shutdown()

	jobCoordinator.runState = shutDown

	jobCoordinator.runStateCond.Broadcast()
}

// WaitForJobCompletion blocks until the specified job has completed.
func (jobCoordinator *JobCoordinator) WaitForJobCompletion(jobName string) error {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	for {
		if jobCoordinator.runState == runningAJob && *jobCoordinator.currentJobName == jobName {
			// Job is currently running, we'll have to wait.
		} else if _, ok := jobCoordinator.jobStatuses[jobName]; ok {
			// Job is completed!
			return nil
		} else {
			// They're trying to wait for a job that was never submitted.
			return errors.New("JobWasNeverSubmitted")
		}

		jobCoordinator.runStateCond.Wait()
	}
}

// WaitForShutdown blocks until the JobCoordinator is shut down.
func (jobCoordinator *JobCoordinator) WaitForShutdown() {
	jobCoordinator.mutex.Lock()
	defer jobCoordinator.mutex.Unlock()

	if jobCoordinator.runState == shutDown {
		return
	}

	for {
		if jobCoordinator.runState == shutDown {
			return
		}

		jobCoordinator.runStateCond.Wait()
	}
}
