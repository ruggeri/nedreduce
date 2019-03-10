package reducer

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// ExecuteReducing runs a reduce task.
func ExecuteReducing(reduceTask *ReduceTask) {
	util.Debug(
		"reduceTaskIdx %v: Beginning reduce task with config: %v.\n",
		reduceTask.ReduceTaskIdx,
		reduceTask,
	)

	// First, we must sort each mapper output file.
	util.Debug(
		"reduceTaskIdx %v: Beginning sorting.\n",
		reduceTask.ReduceTaskIdx,
	)
	sortReducerInputFiles(reduceTask)
	util.Debug(
		"reduceTaskIdx %v: Finished sorting.\n",
		reduceTask.ReduceTaskIdx,
	)

	// Now, open the reducer's input files.
	inputManager := NewInputManager(reduceTask)
	defer inputManager.Close()

	// Open the reducer's output file. Setup the output encoder.
	outputFileName := util.ReducerOutputFileName(
		reduceTask.JobName, reduceTask.ReduceTaskIdx,
	)
	outputFile, err := os.OpenFile(
		outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644,
	)
	if err != nil {
		log.Fatalf("error opening reducer output file: %v\n", err)
	}
	defer outputFile.Close()
	outputEncoder := json.NewEncoder(outputFile)

	util.Debug(
		"reduceTaskIdx %v: Beginning reducing.\n",
		reduceTask.ReduceTaskIdx,
	)

	reducingFunction := reduceTask.ReducingFunction()

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
			func() (*types.KeyValue, error) { return groupIterator.Next() },
			func(outputKeyValue types.KeyValue) {
				// Write out the outputValue.
				err = outputEncoder.Encode(outputKeyValue)
				if err != nil {
					log.Fatalf("reduce output error: %v\n", err)
				}
			},
		)

		// You're supposed to call `Close` when done with a `GroupIterator`.
		// This matters in case the ReducingFunction hasn't fully iterated
		// the `KeyValue`s in the group.
		groupIterator.Close()
	}

	util.Debug(
		"reduceTaskIdx %v: Completed reduce task.\n",
		reduceTask.ReduceTaskIdx,
	)
}
