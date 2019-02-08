package common

import (
	"log"
	"os"
	"strconv"
)

// IntermediateFileName constructs the name of the intermediate file
// which map task <mapTask> produces for reduce task <reduceTask>.
func IntermediateFileName(jobName string, mapTaskIdx int, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-mapper-" + strconv.Itoa(mapTaskIdx) + "output-for-reducer-" + strconv.Itoa(reduceTaskIdx)
}

// ReducerOutputFileName constructs the name of the output file of
// reduce task <reduceTask>
func ReducerOutputFileName(jobName string, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-reducer-" + strconv.Itoa(reduceTaskIdx) + "-output"
}

// CleanupFiles removes all intermediate files produced by running
// mapreduce.
func CleanupFiles(jobName string, mapperInputFileNames string, numReducers int) {
	// Clean up mapper output files.
	for mapTaskIdx := range mapperInputFileNames {
		for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
			removeFile(IntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx))
		}
	}

	// Clean up reducer output files.
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		removeFile(ReducerOutputFileName(jobName, reduceTaskIdx))
	}

	// Clean up final output file.
	removeFile("mrtmp." + jobName)
}

// removeFile is a simple wrapper around os.Remove that logs errors.
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}
