package worker

import "sync"

// TODO: this helper is unreviewed.

// track whether workers executed in parallel.
type Parallelism struct {
	Mu  sync.Mutex
	now int32
	Max int32
}
