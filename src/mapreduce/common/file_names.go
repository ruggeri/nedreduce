package common

import "strconv"

// IntermediateFileName constructs the name of the intermediate file
// which map task <mapTask> produces for reduce task <reduceTask>.
func IntermediateFileName(jobName string, mapTaskIdx int, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-mapper-" + strconv.Itoa(mapTaskIdx) + "output-for-reducer-" + strconv.Itoa(reduceTaskIdx)
}

// OutputFileName constructs the name of the output file of reduce task
// <reduceTask>
func OutputFileName(jobName string, reduceTaskIdx int) string {
	return "mrtmp." + jobName + "-reducer-" + strconv.Itoa(reduceTaskIdx) + "-output"
}
