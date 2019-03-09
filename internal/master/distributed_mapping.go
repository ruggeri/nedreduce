package master

import (
	"log"

	"github.com/ruggeri/nedreduce/internal/master/work_assigner"
	mr_rpc "github.com/ruggeri/nedreduce/internal/rpc"
	"github.com/ruggeri/nedreduce/internal/types"
)

type MapWorkItem struct {
	jobConfiguration *types.JobConfiguration
	mapTaskIdx       int
}

func (mapWorkItem *MapWorkItem) RPCArgs() mr_rpc.DoTaskArgs {
	jobConfiguration := mapWorkItem.jobConfiguration
	jobName := jobConfiguration.JobName
	numMappers := jobConfiguration.NumMappers()
	numReducers := jobConfiguration.NumReducers

	mapInputFileName := jobConfiguration.MapperInputFileNames[mapWorkItem.mapTaskIdx]
	args := mr_rpc.DoTaskArgs{
		JobName:              jobName,
		JobPhase:             types.MapPhase,
		MapInputFileName:     mapInputFileName,
		TaskIdx:              mapWorkItem.mapTaskIdx,
		NumTasksInOtherPhase: numReducers,
	}

	return args
}

func (mapWorkItem *MapWorkItem) StartOnWorker(
	workerAddress string,
	workCompletionCallback work_assigner.WorkCompletionCallback,
) {
	doTaskArgs := mapWorkItem.RPCArgs()

	ok := mr_rpc.Call(workerAddress, "Worker.DoTask", doTaskArgs, nil)

	if !ok {
		log.Fatal("Something went wrong with RPC call to worker.")
	} else {
		workCompletionCallback()
	}
}

type MapWorkProducer struct {
	jobConfiguration *types.JobConfiguration
	nextMapTaskIdx   int
}

func (mapWorkProducer *MapWorkProducer) produceNextWork() *MapWorkItem {
	if mapWorkProducer.nextMapTaskIdx == mapWorkProducer.jobConfiguration.NumMappers() {
		return nil
	}

	mapWorkItem := &MapWorkItem{
		jobConfiguration: mapWorkProducer.jobConfiguration,
		mapTaskIdx:       mapWorkProducer.nextMapTaskIdx,
	}

	mapWorkProducer.nextMapTaskIdx++

	return mapWorkItem
}

func runDistributedMapPhase(master *Master) {
	mapWorkProducer := MapWorkProducer{
		jobConfiguration: master.jobConfiguration,
		nextMapTaskIdx:   0,
	}

	workAssigner := work_assigner.Start(
		func() work_assigner.WorkItem { return mapWorkProducer.produceNextWork() },
		master.workerPoolManager.NewWorkerRPCAddressStream(),
	)
}
