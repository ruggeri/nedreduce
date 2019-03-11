package rpc

import (
	"log"
)

func WaitForJobCompletion(
	jobCoordinatorRPCAddress string,
	jobName string,
) {
	ok := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.WaitForJobCompletion",
		jobName,
		nil,
	)

	if !ok {
		log.Panicf(
			"encountered RPC error while submitting job to JobCoordinator @ %s",
			jobCoordinatorRPCAddress,
		)
	}
}
