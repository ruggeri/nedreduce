package rpc

import "log"

// RegisterWorkerWithJobCoordinator performs an RPC to the
// JobCoordinator so that a Worker can let them know they exist.
func RegisterWorkerWithJobCoordinator(
	jobCoordinatorRPCAddress string,
	workerRPCAddress string,
) {
	err := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.RegisterWorker",
		workerRPCAddress,
		nil,
	)

	if err != nil {
		log.Panicf(
			"worker @ %s encountered RPC error while registering with JobCoordinator @ %s: %v\n",
			workerRPCAddress,
			jobCoordinatorRPCAddress,
			err,
		)
	}
}
