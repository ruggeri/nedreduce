package worker_pool

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// A WorkProducingFunction is how a user of the WorkerPool produces
// Tasks to be assigned to workers. When there are no more tasks to be
// assigned, the WorkProducingFunction should return EOF.
type WorkProducingFunction func() (mr_rpc.Task, error)
