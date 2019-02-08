package reducer

import (
	"encoding/json"
	"log"
	"mapreduce/common"
	"os"
)

// A InputManager manages the mapper output files that constitute the
// input for this reduce task.
type InputManager struct {
	inputFiles    []os.File
	inputDecoders []*json.Decoder
}

// NewInputManager opens the appropriate files, sets up the decoders,
// and builds a ReducerInputManager.
func NewInputManager(configuration Configuration) InputManager {
	jobName := configuration.JobName
	numMappers := configuration.NumMappers
	reduceTaskIdx := configuration.ReduceTaskIdx

	inputManager := InputManager{
		inputFiles:    make([]os.File, 0, numMappers),
		inputDecoders: make([]*json.Decoder, 0, numMappers),
	}

	// For each map task...
	for mapTaskIdx := 0; mapTaskIdx < numMappers; mapTaskIdx++ {
		// Open an input file and append it to the list.
		inputFileName := common.IntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			log.Fatalf("error opening reducer input file: %v\n", err)
		}
		inputManager.inputFiles = append(inputManager.inputFiles, *inputFile)

		// Then prepare a JSON decoder so we can read KeyValues in a nice
		// format.
		inputDecoder := json.NewDecoder(inputFile)
		inputManager.inputDecoders = append(inputManager.inputDecoders, inputDecoder)
	}

	return inputManager
}

// Close closes all the reducer input files.
func (inputManager *InputManager) Close() {
	for _, inputFile := range inputManager.inputFiles {
		inputFile.Close()
	}
}
