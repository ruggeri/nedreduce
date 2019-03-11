package rpc

import "log"

func RegisterWorkerWithJobCoordinator(
	jobCoordinatorRPCAddress string,
	workerRPCAddress string,
) {
	ok := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.RegisterWorker",
		workerRPCAddress,
		nil,
	)

	if !ok {
		log.Panic(
			"worker @ %s encountered RPC error while registering with JobCoordinator @ %s",
			workerRPCAddress,
			jobCoordinatorRPCAddress,
		)
	}
}
