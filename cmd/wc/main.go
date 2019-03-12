package main

import (
	"log"
	"os"
	"strconv"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

func main() {
	command := os.Args[1]

	switch command {
	case "run-coordinator":
		runCoordinator()
	case "run-worker":
		runWorker()
	case "shutdown-coordinator":
		shutdownCoordinator()
	case "shutdown-worker":
		shutdownWorker()
	case "submit-job":
		submitJob()
	default:
		log.Fatalf("Unrecognized command: %v\n", command)
	}
}

func runCoordinator() {
	if len(os.Args) != 3 {
		log.Fatal("wc run-coordinator jobCoordinatorAddress")
	}
	jobCoordinatorAddress := os.Args[2]
	nedreduce.RunJobCoordinator(jobCoordinatorAddress)
}

func runWorker() {
	if len(os.Args) != 4 {
		log.Fatal("wc run-worker jobCoordinatorAddress workerAddress")
	}

	jobCoordinatorAddress := os.Args[2]
	workerAddress := os.Args[3]

	nedreduce.RunWorker(jobCoordinatorAddress, workerAddress)
}

func shutdownCoordinator() {
	if len(os.Args) != 3 {
		log.Fatal("wc shutdown-coordinator jobCoordinatorAddress")
	}
	jobCoordinatorAddress := os.Args[2]
	nedreduce.ShutdownJobCoordinator(jobCoordinatorAddress)
}

func shutdownWorker() {
	if len(os.Args) != 3 {
		log.Fatal("wc shutdown-worker workerRPCAddress")
	}

	workerRPCAddress := os.Args[2]

	nedreduce.ShutdownWorker(workerRPCAddress)
}

func submitJob() {
	if len(os.Args) < 6 {
		log.Fatal("wc submit-job jobCoordinatorAddress executionMode numReducers inputFiles...")
	}

	jobCoordinatorAddress := os.Args[2]
	executionModeName := os.Args[3]
	numReducers, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Printf("invalid numReducers: %v\n", os.Args[4])
		log.Fatal("wc submit-job jobCoordinatorAddress executionMode numReducers inputFiles...")
	}
	inputFiles := os.Args[5:]

	var executionMode nedreduce.ExecutionMode
	switch executionModeName {
	case "sequential":
		executionMode = nedreduce.Sequential
	case "distributed":
		executionMode = nedreduce.Distributed
	default:
		log.Panicf("Unexpected execution mode: %v\n", executionModeName)
	}

	jobName := "wcseq"
	mappingFunctionName := "WordSplittingMappingFunction"
	reducingFunctionName := "WordCountingReducingFunction"

	jobConfiguration := nedreduce.NewJobConfiguration(
		jobName,
		inputFiles,
		numReducers,
		mappingFunctionName,
		reducingFunctionName,
		executionMode,
	)

	nedreduce.SubmitJob(jobCoordinatorAddress, jobConfiguration)
	nedreduce.WaitForJobCompletion(jobCoordinatorAddress, jobName)
}

func waitUntilFileExists() {
	if _, err := os.Stat("/path/to/whatever"); os.IsNotExist(err) {
		// path/to/whatever does not exist
	}
}
