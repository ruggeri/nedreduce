package common

import (
	"log"
	"os"
	"strconv"
)

// IntermediateFileName constructs the name of the intermediate file
// which map task <mapTask> produces for reduce task <reduceTask>.
func IntermediateFileName(jobName string, mapTaskIdx int, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-mapper-" + strconv.Itoa(mapTaskIdx) + "-output-for-reducer-" + strconv.Itoa(reduceTaskIdx)
}

// ReducerOutputFileName constructs the name of the output file of
// reduce task <reduceTask>
func ReducerOutputFileName(jobName string, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-reducer-" + strconv.Itoa(reduceTaskIdx) + "-output"
}

// RemoveFile is a simple wrapper around os.Remove that logs errors.
func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}
