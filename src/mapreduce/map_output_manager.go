package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

// MapOutputManager manages the many output files of a single map task.
type MapOutputManager struct {
	outputFiles    []os.File
	outputEncoders []*json.Encoder
}

// NewMapOutputManager makes a new MapOutputManager.
func NewMapOutputManager(jobName string, mapTask int, numReducers int) MapOutputManager {
	// Allocate space for slices.
	mapOutputManager := MapOutputManager{
		outputFiles:    make([]os.File, 0, numReducers),
		outputEncoders: make([]*json.Encoder, 0, numReducers),
	}

	// For each reduce task...
	for reduceTask := 0; reduceTask < numReducers; reduceTask++ {
		// Open an output file and append it to the list.
		outputFileName := reduceName(jobName, mapTask, reduceTask)
		outputFile, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("error opening mapper output file: %v\n", err)
		}
		mapOutputManager.outputFiles = append(mapOutputManager.outputFiles, *outputFile)

		// Then prepare a JSON encoder so we can write KeyValues in a nice
		// format.
		outputEncoder := json.NewEncoder(outputFile)
		mapOutputManager.outputEncoders = append(mapOutputManager.outputEncoders, outputEncoder)
	}

	return mapOutputManager
}

// WriteKeyValue calculates which reduce task the KeyValue should be
// written to, and then writes to the appropriate file.
func (mapOutputManager *MapOutputManager) WriteKeyValue(keyValue KeyValue) {
	reducerIdx := ihash(keyValue.Key) % len(mapOutputManager.outputFiles)
	err := mapOutputManager.outputEncoders[reducerIdx].Encode(keyValue)
	if err != nil {
		log.Fatalf("unexpected map output error: %v\n", err)
	}
}

// Close iterates the map task output files and closes each.
func (mapOutputManager *MapOutputManager) Close() {
	for _, outputFile := range mapOutputManager.outputFiles {
		outputFile.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
