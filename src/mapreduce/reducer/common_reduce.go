package reducer

import (
	"encoding/json"
	"io"
	"log"
	"mapreduce/common"
	"os"
)

// A GroupIteratorFunction is how a ReducingFunction is passed the
// KeyValues that constitute a reduce group.
type GroupIteratorFunction func() (*common.KeyValue, error)

// A ReducingEmitterFunction is how a ReducingFunction outputs
// KeyValues.
type ReducingEmitterFunction func(outputKeyValue common.KeyValue)

// A ReducingFunction is the type of function the user supplies to do
// the reducing.
type ReducingFunction func(
	groupKey string,
	groupIteratorFunction GroupIteratorFunction,
	reducingEmitterFunction ReducingEmitterFunction,
)

// ExecuteReducing runs a reduce task.
func ExecuteReducing(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIdx int, // which reduce task this is
	numMappers int, // the number of map tasks that were run ("M" in the paper)
	reducingFunction ReducingFunction,
) {
	// First, we must sort each mapper output file.
	sortReducerInputFiles(jobName, numMappers, reduceTaskIdx)

	// Now, open the reducer's input files.
	inputManager := NewInputManager(jobName, numMappers, reduceTaskIdx)
	defer inputManager.Close()

	// Open the reducer's output file. Setup the output encoder.
	outputFileName := common.ReducerOutputFileName(jobName, reduceTaskIdx)
	outputFile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("error opening reducer output file: %v\n", err)
	}
	defer outputFile.Close()
	outputEncoder := json.NewEncoder(outputFile)

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
		reducingFunction(
			groupIterator.GroupKey,
			func() (*common.KeyValue, error) { return groupIterator.Next() },
			func(outputKeyValue common.KeyValue) {
				// Write out the outputValue.
				err = outputEncoder.Encode(outputKeyValue)
				if err != nil {
					log.Fatalf("reduce output error: %v\n", err)
				}
			},
		)
	}
}
