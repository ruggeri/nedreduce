package util

import (
	"os"
	"strconv"

	"github.com/ruggeri/nedreduce/internal/types"
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

// CleanupFiles removes all intermediate files produced by running
// mapreduce.
func CleanupFiles(configuration *types.JobConfiguration) {
	jobName := configuration.JobName
	mapperInputFileNames := configuration.MapperInputFileNames
	numReducers := configuration.NumReducers

	// Clean up mapper output files.
	for mapTaskIdx := range mapperInputFileNames {
		for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
			fileName := IntermediateFileName(
				jobName, mapTaskIdx, reduceTaskIdx,
			)
			os.Remove(fileName)
		}
	}

	// Clean up reducer output files.
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		fileName := ReducerOutputFileName(jobName, reduceTaskIdx)
		os.Remove(fileName)
	}
}
