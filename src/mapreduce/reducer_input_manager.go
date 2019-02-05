package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// ReducerInputManager manages the mapper output files for this reduce
// task.
type ReducerInputManager struct {
	inputFiles    []os.File
	inputDecoders []*json.Decoder
}

// NewReducerInputManager opens the appropriate files, sets up the
// decoders, and builds a ReducerInputManager.
func NewReducerInputManager(jobName string, numMappers int, reduceTask int) ReducerInputManager {
	reducerInputManager := ReducerInputManager{
		inputFiles:    make([]os.File, 0, numMappers),
		inputDecoders: make([]*json.Decoder, 0, numMappers),
	}

	// For each map task...
	for mapperIdx := 0; mapperIdx < numMappers; mapperIdx++ {
		// Open an input file and append it to the list.
		inputFileName := reduceName(jobName, mapperIdx, reduceTask)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			log.Fatalf("error opening reducer input file: %v\n", err)
		}
		reducerInputManager.inputFiles = append(reducerInputManager.inputFiles, *inputFile)

		// Then prepare a JSON decoder so we can read KeyValues in a nice
		// format.
		inputDecoder := json.NewDecoder(inputFile)
		reducerInputManager.inputDecoders = append(reducerInputManager.inputDecoders, inputDecoder)
	}

	return reducerInputManager
}

// Close closes all the reducer input files.
func (reducerInputManager *ReducerInputManager) Close() {
	for _, inputFile := range reducerInputManager.inputFiles {
		inputFile.Close()
	}
}
