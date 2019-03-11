package rpc

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/types"
)

func SubmitJob(
	jobCoordinatorRPCAddress string,
	jobConfiguration *types.JobConfiguration,
) {
	err := Call(
		jobCoordinatorRPCAddress,
		"JobCoordinator.StartJob",
		jobConfiguration,
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
