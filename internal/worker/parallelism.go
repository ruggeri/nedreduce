package worker

import "sync"

// track whether workers executed in parallel.
type Parallelism struct {
	Mu  sync.Mutex
	now int32
	Max int32
}
