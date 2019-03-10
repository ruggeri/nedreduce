package work_assigner

import (
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
)

// A WorkProducingFunction is how a user of the WorkAssigner produces
// WorkItems to be assigned. When there is no more work to be assigned,
// the WorkProducingFunction should return nil.
type WorkProducingFunction func() mr_rpc.Task
