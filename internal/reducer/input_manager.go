package reducer

import (
	"bufio"
	"encoding/json"
	"log"
	"os"

	"github.com/ruggeri/nedreduce/internal/util"
)

// A InputManager manages the mapper output files that constitute the
// input for this reduce task.
type InputManager struct {
	inputFiles        []os.File
	inputBufioReaders []*bufio.Reader
	inputDecoders     []*json.Decoder
}

// NewInputManager opens the appropriate files, sets up the decoders,
// and builds a ReducerInputManager.
func NewInputManager(reduceTask *ReduceTask) InputManager {
	jobName := reduceTask.JobName
	numMappers := reduceTask.NumMappers
	reduceTaskIdx := reduceTask.ReduceTaskIdx

	inputManager := InputManager{
		inputFiles:        make([]os.File, 0, numMappers),
		inputBufioReaders: make([]*bufio.Reader, 0, numMappers),
		inputDecoders:     make([]*json.Decoder, 0, numMappers),
	}

	// For each map task...
	for mapTaskIdx := 0; mapTaskIdx < numMappers; mapTaskIdx++ {
		// Open an input file and append it to the list.
		inputFileName := util.IntermediateFileName(
			jobName, mapTaskIdx, reduceTaskIdx,
		)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			log.Panicf("error opening reducer input file: %v\n", err)
		}
		inputManager.inputFiles = append(
			inputManager.inputFiles, *inputFile,
		)

		// We'll buffer reading for better performance.
		inputBufioReader := bufio.NewReader(inputFile)
		inputManager.inputBufioReaders = append(
			inputManager.inputBufioReaders, inputBufioReader,
		)

		// Then prepare a JSON decoder so we can read KeyValues in a nice
		// format.
		inputDecoder := json.NewDecoder(inputBufioReader)
		inputManager.inputDecoders = append(
			inputManager.inputDecoders, inputDecoder,
		)
	}

	return inputManager
}

// Close closes all the reducer input files.
func (inputManager *InputManager) Close() {
	for _, inputFile := range inputManager.inputFiles {
		inputFile.Close()
	}
}
