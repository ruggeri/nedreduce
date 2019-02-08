package reducer

import (
	"encoding/json"
	"io"
	"log"
	"mapreduce/common"
	. "mapreduce/types"
	"os"
	"sort"
)

func sortReducerInputFiles(configuration Configuration) {
	// Open the reducer's input files.
	reducerInputManager := NewInputManager(configuration)
	defer reducerInputManager.Close()

	for mapTaskIdx, inputDecoder := range reducerInputManager.inputDecoders {
		// Sort each file.
		sortReducerInputFile(
			configuration,
			mapTaskIdx,
			reducerInputManager.inputFiles[mapTaskIdx],
			inputDecoder,
		)
	}
}

func sortReducerInputFile(
	configuration Configuration,
	mapTaskIdx int,
	inputReadingFile os.File,
	inputDecoder *json.Decoder,
) {
	jobName := configuration.JobName
	reduceTaskIdx := configuration.ReduceTaskIdx

	// Read in all KeyValues for this mapper input. Gross.
	//
	// TODO: Change this into an external merge sort.
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
	inputReadingFile.Close()

	// Now do the sorting.
	sort.SliceStable(keyValues, func(i, j int) bool {
		return keyValues[i].Key < keyValues[j].Key
	})

	// Open a new file for writing. Really I should be creating a new file
	// and then re-naming.
	inputFileName := common.IntermediateFileName(
		jobName, mapTaskIdx, reduceTaskIdx,
	)
	inputWritingFile, err := os.OpenFile(
		inputFileName, os.O_TRUNC|os.O_WRONLY, 0644,
	)
	if err != nil {
		log.Fatalf("unexpected error re-opening %v\n", err)
	}
	defer inputWritingFile.Close()

	encoder := json.NewEncoder(inputWritingFile)
	for _, keyValue := range keyValues {
		err := encoder.Encode(keyValue)
		if err != nil {
			log.Fatalf("unexpected error encoding KeyValue: %v\n", err)
		}
	}
}
