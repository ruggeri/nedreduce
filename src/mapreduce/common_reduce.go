package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	doSort(jobName, nMap, reduceTask)

	// Open the reducer's input files.
	reducerInputManager := NewReducerInputManager(jobName, nMap, reduceTask)
	defer reducerInputManager.Close()

	// Open the reducer's output file. Setup the output encoder.
	outputFile, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

func doSort(jobName string, numMappers int, reduceTaskIdx int) {
	// TODO: Change this into an external merge sort.

	// Open the reducer's input files.
	reducerInputManager := NewReducerInputManager(jobName, numMappers, reduceTaskIdx)
	defer reducerInputManager.Close()

	for mapTaskIdx, inputDecoder := range reducerInputManager.inputDecoders {
		// Read in all KeyValues for this mapper input. Gross.
		keyValues := []KeyValue{}
		for {
			keyValue := &KeyValue{}
			err := inputDecoder.Decode(keyValue)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("unexpected decode error: %v\n", err)
			}

			keyValues = append(keyValues, *keyValue)
		}

		// Close the file.
		reducerInputManager.inputFiles[mapTaskIdx].Close()

		// Now do the sorting.
		sort.SliceStable(keyValues, func(i, j int) bool {
			return keyValues[i].Key < keyValues[j].Key
		})

		// Open a new file for writing. Really I should be creating a new
		// file and then re-naming.
		file, err := os.OpenFile(reduceName(jobName, mapTaskIdx, reduceTaskIdx), os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("unexpected error re-opening %v\n", err)
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		for _, keyValue := range keyValues {
			err := encoder.Encode(keyValue)
			if err != nil {
				log.Fatalf("unexpected error encoding KeyValue: %v\n", err)
			}
		}
	}
}
