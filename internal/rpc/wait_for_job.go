package rpc

import (
	"log"
)

// WaitForJobCompletion performs an RPC which blocks until the
// JobCoordinator has finished executing the job.
func WaitForJobCompletion(
	jobCoordinatorRPCAddress string,
	jobName string,
) {
	err := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.WaitForJobCompletion",
		jobName,
		nil,
	)

	if err != nil {
		log.Panicf(
			"encountered RPC error while submitting job to JobCoordinator @ %s: %v\n",
			jobCoordinatorRPCAddress,
			err,
		)
	}
}
