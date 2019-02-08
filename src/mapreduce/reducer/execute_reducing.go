package reducer

import (
	"encoding/json"
	"io"
	"log"
	"mapreduce/common"
	. "mapreduce/types"
	"os"
)

// ExecuteReducing runs a reduce task.
func ExecuteReducing(configuration *Configuration) {
	common.Debug(
		"reduceTaskIdx %v: Beginning reduce task with config: %v.\n",
		configuration.ReduceTaskIdx,
		configuration,
	)

	// First, we must sort each mapper output file.
	common.Debug(
		"reduceTaskIdx %v: Beginning sorting.\n",
		configuration.ReduceTaskIdx,
	)
	sortReducerInputFiles(configuration)
	common.Debug(
		"reduceTaskIdx %v: Finished sorting.\n",
		configuration.ReduceTaskIdx,
	)

	// Now, open the reducer's input files.
	inputManager := NewInputManager(configuration)
	defer inputManager.Close()

	// Open the reducer's output file. Setup the output encoder.
	outputFileName := common.ReducerOutputFileName(
		configuration.JobName, configuration.ReduceTaskIdx,
	)
	outputFile, err := os.OpenFile(
		outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644,
	)
	if err != nil {
		log.Fatalf("error opening reducer output file: %v\n", err)
	}
	defer outputFile.Close()
	outputEncoder := json.NewEncoder(outputFile)

	common.Debug(
		"reduceTaskIdx %v: Beginning reducing.\n",
		configuration.ReduceTaskIdx,
	)
	// Iterate the groups one at a time.
	groupingIterator := NewGroupingIterator(inputManager.inputDecoders)
	for {
		groupIterator, err := groupingIterator.Next()

		if err == io.EOF {
			// No more groups.
			break
		} else if err != nil {
			log.Fatalf("unexpected groupingIterator error: %v\n", err)
		}

		// Call the reducer function.
		configuration.ReducingFunction(
			groupIterator.GroupKey,
			func() (*KeyValue, error) { return groupIterator.Next() },
			func(outputKeyValue KeyValue) {
				// Write out the outputValue.
				err = outputEncoder.Encode(outputKeyValue)
				if err != nil {
					log.Fatalf("reduce output error: %v\n", err)
				}
			},
		)
	}

	common.Debug(
		"reduceTaskIdx %v: Completed reduce task.\n",
		configuration.ReduceTaskIdx,
	)
}
