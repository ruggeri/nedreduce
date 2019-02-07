package reducer

import (
	"encoding/json"
	"io"
	"log"
	"mapreduce/common"
	"os"
	"sort"
)

func sortReducerInputFiles(jobName string, numMappers int, reduceTaskIdx int) {
	// TODO: Change this into an external merge sort.

	// Open the reducer's input files.
	reducerInputManager := NewInputManager(jobName, numMappers, reduceTaskIdx)
	defer reducerInputManager.Close()

	for mapTaskIdx, inputDecoder := range reducerInputManager.inputDecoders {
		// Read in all KeyValues for this mapper input. Gross.
		keyValues := []common.KeyValue{}
		for {
			keyValue := &common.KeyValue{}
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
		file, err := os.OpenFile(common.IntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx), os.O_TRUNC|os.O_WRONLY, 0644)
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
