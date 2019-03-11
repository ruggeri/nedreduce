package rpc

import "log"

// ShutdownWorker tells a Worker to shutdown. It returns the number of
// tasks this worker has processed since it was started.
func ShutdownWorker(workerRPCAddress string) int {
	numTasksProcessed := 0
	err := Call(
		workerRPCAddress,
		"Worker.Shutdown",
		nil,
		&numTasksProcessed,
	)

	if err != nil {
		log.Panicf(
			"JobCoordinator encountered RPC error while shutting down Worker @ %s: %v\n",
			workerRPCAddress,
			err,
		)
	}

	return numTasksProcessed
}
