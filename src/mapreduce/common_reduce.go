package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO: must sort the mapper output files.

	// Open the reducer's input files.
	reducerInputManager := NewReducerInputManager(jobName, nMap, reduceTask)
	defer reducerInputManager.Close()

	// Open the reducer's output file. Setup the output encoder.
	outputFile, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("error opening reducer output file: %v\n", err)
	}
	defer outputFile.Close()
	outputEncoder := json.NewEncoder(outputFile)

	// Iterate the groups one at a time.
	groupingIterator := NewGroupingIterator(reducerInputManager.inputDecoders)
	for {
		groupIterator, err := groupingIterator.Next()

		if err == io.EOF {
			// No more groups.
			break
		} else if err != nil {
			log.Fatalf("unexpected groupingIterator error: %v\n", err)
		}

		// TODO: I would rather pass in the iterator to the reducer
		// function, but for now I will materialize the entire group in
		// memory.
		groupKey := groupIterator.GroupKey
		values := []string{}
		for {
			keyValue, err := groupIterator.Next()

			if err == io.EOF {
				// This group is over!
				break
			} else if err != nil {
				log.Fatalf("unexpected GroupIterator error: %v\n", err)
			}

			values = append(values, keyValue.Value)
		}

		// Call the reducer function. TODO: a reducer can typically produce
		// more than one output row per group if desired.
		outputValue := reduceF(groupKey, values)
		outputKeyValue := KeyValue{groupIterator.GroupKey, outputValue}

		// Write out the outputValue.
		err = outputEncoder.Encode(outputKeyValue)
		if err != nil {
			log.Fatalf("reduce output error: %v\n", err)
		}
	}
}
