package reducer

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"

	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// Helper function to iterate the input files to the reducer, sorting
// each one by key. This is needed because mappers don't emit rows in
// any particular order.
func sortReducerInputFiles(reduceTask *ReduceTask) {
	// Open the reducer's input files.
	reducerInputManager := NewInputManager(reduceTask)
	defer reducerInputManager.Close()

	for mapTaskIdx, inputDecoder := range reducerInputManager.inputDecoders {
		// Sort each file.
		sortReducerInputFile(
			reduceTask,
			mapTaskIdx,
			reducerInputManager.inputFiles[mapTaskIdx],
			inputDecoder,
		)
	}
}

func sortReducerInputFile(
	reduceTask *ReduceTask,
	mapTaskIdx int,
	inputReadingFile os.File,
	inputDecoder *json.Decoder,
) {
	jobName := reduceTask.JobName
	reduceTaskIdx := reduceTask.ReduceTaskIdx

	// Read in all KeyValues for this mapper input. Gross.
	//
	// TODO(LOW): Change this into an external merge sort.
	keyValues := []types.KeyValue{}
	for {
		keyValue := &types.KeyValue{}
		err := inputDecoder.Decode(keyValue)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Panicf("unexpected decode error: %v\n", err)
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
	inputFileName := util.IntermediateFileName(
		jobName, mapTaskIdx, reduceTaskIdx,
	)
	inputWritingFile, err := os.OpenFile(
		inputFileName, os.O_TRUNC|os.O_WRONLY, 0644,
	)
	if err != nil {
		log.Panicf("unexpected error re-opening %v\n", err)
	}
	defer inputWritingFile.Close()

	// Use buffered writing for better performance.
	inputFileBufioWriter := bufio.NewWriter(inputWritingFile)
	defer inputFileBufioWriter.Flush()

	// Setup the encoder.
	encoder := json.NewEncoder(inputFileBufioWriter)

	// And now write all sorted data back out.
	for _, keyValue := range keyValues {
		err := encoder.Encode(keyValue)
		if err != nil {
			log.Panicf("unexpected error encoding KeyValue: %v\n", err)
		}
	}
}
