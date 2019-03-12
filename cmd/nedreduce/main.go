package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

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
		log.Fatal("nedreduce run-coordinator jobCoordinatorAddress")
	}
	jobCoordinatorAddress := os.Args[2]
	nedreduce.RunJobCoordinator(jobCoordinatorAddress)
}

func runWorker() {
	if len(os.Args) != 4 {
		log.Fatal("nedreduce run-worker jobCoordinatorAddress workerAddress")
	}

	jobCoordinatorAddress := os.Args[2]
	workerAddress := os.Args[3]

	nedreduce.RunWorker(jobCoordinatorAddress, workerAddress)
}

func shutdownCoordinator() {
	if len(os.Args) != 3 {
		log.Fatal("nedreduce shutdown-coordinator jobCoordinatorAddress")
	}
	jobCoordinatorAddress := os.Args[2]
	nedreduce.ShutdownJobCoordinator(jobCoordinatorAddress)
}

func shutdownWorker() {
	if len(os.Args) != 3 {
		log.Fatal("nedreduce shutdown-worker workerRPCAddress")
	}

	workerRPCAddress := os.Args[2]

	nedreduce.ShutdownWorker(workerRPCAddress)
}

func submitJob() {
	if len(os.Args) != 4 {
		log.Fatal("nedreduce submit-job jobCoordinatorAddress jobConfigurationJson")
	}

	jobCoordinatorAddress := os.Args[2]

	jobConfiguration := func() *nedreduce.JobConfiguration {
		configFname := os.Args[3]
		var jobConfiguration nedreduce.JobConfiguration
		configContents, _ := ioutil.ReadFile(configFname)
		json.Unmarshal(configContents, &jobConfiguration)

		return &jobConfiguration
	}()

	nedreduce.SubmitJob(jobCoordinatorAddress, jobConfiguration)
	nedreduce.WaitForJobCompletion(
		jobCoordinatorAddress,
		jobConfiguration.JobName,
	)
}

func waitUntilFileExists() {
	if _, err := os.Stat("/path/to/whatever"); os.IsNotExist(err) {
		// path/to/whatever does not exist
	}
}
