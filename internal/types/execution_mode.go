package types

// ExecutionMode specifies how a JobCoordinator should execute a job.
type ExecutionMode string

const (
	// Sequential means execute the job right at the JobCoordinator, using
	// no Workers.
	Sequential = ExecutionMode("Sequential")

	// Distributed means that the JobCoordinator should hand out tasks to
	// individual Workers to achieve parallelism and distribution.
	Distributed = ExecutionMode("Distributed")
)
