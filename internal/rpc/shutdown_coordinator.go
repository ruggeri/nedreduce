package rpc

import (
	"log"
)

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
