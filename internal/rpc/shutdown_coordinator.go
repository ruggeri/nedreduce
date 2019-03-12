package rpc

import (
	"log"
)

// ShutdownJobCoordinator performs an RPC to tell the Coordinator to
// shut itself down.
func ShutdownJobCoordinator(
	jobCoordinatorRPCAddress string,
) {
	err := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.Shutdown",
		&struct{}{},
		nil,
	)

	if err != nil {
		log.Panicf(
			"encountered RPC error while shutting down JobCoordinator @ %s: %v\n",
			jobCoordinatorRPCAddress,
			err,
		)
	}
}
