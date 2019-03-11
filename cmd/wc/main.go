package main

import (
	"log"
	"os"
	"strconv"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go coordinator sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go coordinator localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	command := os.Args[1]

	switch command {
	case "run-coordinator":
		if len(os.Args) != 3 {
			log.Fatal("wc run-coordinator jobCoordinatorAddress")
		}
		jobCoordinatorAddress := os.Args[2]
		nedreduce.RunJobCoordinator(jobCoordinatorAddress)
	case "shutdown-coordinator":
		if len(os.Args) != 3 {
			log.Fatal("wc shutdown-coordinator jobCoordinatorAddress")
		}
		jobCoordinatorAddress := os.Args[2]
		nedreduce.ShutdownJobCoordinator(jobCoordinatorAddress)
	case "submit-job":
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
	case "run-worker":
		if len(os.Args) != 4 {
			log.Fatal("wc run-worker jobCoordinatorAddress workerAddress")
		}

		jobCoordinatorAddress := os.Args[2]
		workerAddress := os.Args[3]

		nedreduce.RunWorker(jobCoordinatorAddress, workerAddress)
	case "shutdown-worker":
		if len(os.Args) != 3 {
			log.Fatal("wc shutdown-worker workerRPCAddress")
		}

		workerRPCAddress := os.Args[2]

		nedreduce.ShutdownWorker(workerRPCAddress)
	default:
		log.Fatalf("Unrecognized command: %v\n", command)
	}
}
