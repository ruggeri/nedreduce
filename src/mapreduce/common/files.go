package common

import (
	"log"
	"os"
	"strconv"
)

// IntermediateFileName constructs the name of the intermediate file
// which map task <mapTask> produces for reduce task <reduceTask>.
func IntermediateFileName(
	jobName string, mapTaskIdx int, reduceTaskIdx int,
) string {
	fileName := "mrtmp." + jobName
	fileName += "-mapper-" + strconv.Itoa(mapTaskIdx)
	fileName += "-output-for-reducer-" + strconv.Itoa(reduceTaskIdx)

	return fileName
}

// ReducerOutputFileName constructs the name of the output file of
// reduce task <reduceTask>
func ReducerOutputFileName(jobName string, reduceTaskIdx int) string {
	fileName := "mrtmp." + jobName
	fileName += "-reducer-" + strconv.Itoa(reduceTaskIdx) + "-output"

	return fileName
}

// RemoveFile is a simple wrapper around os.Remove that logs errors.
func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}
