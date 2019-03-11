package worker

import (
	"sync"
	"time"
)

// ParallelismTester is used by test code to check whether true
// parallelism is exhibited.
type ParallelismTester struct {
	mutex                     sync.Mutex
	currentLevelOfParallelism int
	maxLevelOfParallelism     int
}

// OnTaskStart is called when a Worker starts a new task. It updates the
// current level of parallelism, and maybe even sleeps the task (so it
// can create more possibility to observe parallelism).
func (parallelismTester *ParallelismTester) OnTaskStart(worker *Worker) {
	pauseForParallelismTesting := func() bool {
		parallelismTester.mutex.Lock()
		defer parallelismTester.mutex.Unlock()

		// Starting a new task increases current level of parallelism.
		parallelismTester.currentLevelOfParallelism++

		// Is this a new maximum level of parallelism?
		if parallelismTester.currentLevelOfParallelism > parallelismTester.maxLevelOfParallelism {
			parallelismTester.maxLevelOfParallelism = parallelismTester.currentLevelOfParallelism
		}

		// If we've never established that any tasks run in parallel on
		// different Workers, pause this task to create opportunity for
		// overlap.
		if parallelismTester.maxLevelOfParallelism < 2 {
			return true
		}

		// Once we know there is *any* parallelism, that's good enough for
		// this test.
		return false
	}()

	if pauseForParallelismTesting {
		// give other workers a chance to prove that
		// they are executing in parallel.
		time.Sleep(time.Second)
	}
}

// OnTaskEnd is called when a Worker has finished a task so that we
// decrement the level of current parallelism.
func (parallelismTester *ParallelismTester) OnTaskEnd(worker *Worker) {
	parallelismTester.mutex.Lock()
	defer parallelismTester.mutex.Unlock()

	parallelismTester.currentLevelOfParallelism--
}

// MaxLevelOfParallelism gives coordinated access to
// maxLevelOfParallelism.
func (parallelismTester *ParallelismTester) MaxLevelOfParallelism() int {
	parallelismTester.mutex.Lock()
	defer parallelismTester.mutex.Unlock()
	return parallelismTester.maxLevelOfParallelism
}
