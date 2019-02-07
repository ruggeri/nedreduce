package mapper

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"mapreduce/common"
	"os"
)

// A OutputManager manages the many output files of a single map task.
type OutputManager struct {
	outputFiles    []os.File
	outputEncoders []*json.Encoder
}

// NewOutputManager makes a new OutputManager.
func NewOutputManager(jobName string, mapTaskIdx int, numReducers int) OutputManager {
	// Allocate space for slices.
	outputManager := OutputManager{
		outputFiles:    make([]os.File, 0, numReducers),
		outputEncoders: make([]*json.Encoder, 0, numReducers),
	}

	// For each reduce task...
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		// Open a mapper output file and append it to the list.
		outputFileName := common.IntermediateFileName(jobName, mapTaskIdx, reduceTaskIdx)
		outputFile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("error opening mapper output file: %v\n", err)
		}
		outputManager.outputFiles = append(outputManager.outputFiles, *outputFile)

		// Then prepare a JSON encoder so we can write KeyValues in a nice
		// format.
		outputEncoder := json.NewEncoder(outputFile)
		outputManager.outputEncoders = append(outputManager.outputEncoders, outputEncoder)
	}

	return outputManager
}

// WriteKeyValue calculates which reduce task the KeyValue should be
// written to, and then writes to the appropriate file.
func (outputManager *OutputManager) WriteKeyValue(keyValue common.KeyValue) {
	reducerIdx := ihash(keyValue.Key) % len(outputManager.outputFiles)
	err := outputManager.outputEncoders[reducerIdx].Encode(keyValue)
	if err != nil {
		log.Fatalf("unexpected map output error: %v\n", err)
	}
}

// Close iterates the map task output files and closes each.
func (outputManager *OutputManager) Close() {
	for _, outputFile := range outputManager.outputFiles {
		outputFile.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
