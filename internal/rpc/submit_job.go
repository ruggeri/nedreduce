package rpc

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/types"
)

func SubmitJob(
	jobCoordinatorRPCAddress string,
	jobConfiguration *types.JobConfiguration,
) {
	ok := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.StartJob",
		jobConfiguration,
		nil,
	)

	if !ok {
		log.Panicf(
			"encountered RPC error while submitting job to JobCoordinator @ %s",
			jobCoordinatorRPCAddress,
		)
	}
}
