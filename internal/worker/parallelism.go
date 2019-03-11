package worker

import (
	"sync"
	"time"
)

// track whether workers executed in parallel.
type Parallelism struct {
	mutex                     sync.Mutex
	currentLevelOfParallelism int
	maxLevelOfParallelism     int
}

// OnTaskStart is called when a Worker starts a new task. It updates the
// current level of parallelism, and maybe even sleeps the task (so it
// can create more possibility to observe parallelism).
func (parallelism *Parallelism) OnTaskStart(worker *Worker) {
	pauseForParallelismTesting := func() bool {
		parallelism.mutex.Lock()
		defer parallelism.mutex.Unlock()

		// Starting a new task increases current level of parallelism.
		parallelism.currentLevelOfParallelism++

		// Is this a new maximum level of parallelism?
		if parallelism.currentLevelOfParallelism > parallelism.maxLevelOfParallelism {
			parallelism.maxLevelOfParallelism = parallelism.currentLevelOfParallelism
		}

		// If we've never established that any tasks run in parallel on
		// different Workers, pause this task to create opportunity for
		// overlap.
		if parallelism.maxLevelOfParallelism < 2 {
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
func (parallelism *Parallelism) OnTaskEnd(worker *Worker) {
	parallelism.mutex.Lock()
	defer parallelism.mutex.Unlock()

	parallelism.currentLevelOfParallelism--
}

// MaxLevelOfParallelism gives coordinated access to
// maxLevelOfParallelism.
func (parallelism *Parallelism) MaxLevelOfParallelism() int {
	parallelism.mutex.Lock()
	defer parallelism.mutex.Unlock()
	return parallelism.maxLevelOfParallelism
}
