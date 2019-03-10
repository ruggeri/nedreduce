package mapper

import (
	"bufio"
	"io"
	"log"
	"os"

	"github.com/ruggeri/nedreduce/internal/types"
	"github.com/ruggeri/nedreduce/internal/util"
)

// ExecuteMapping runs a map task.
func ExecuteMapping(mapTask *MapTask) {
	util.Debug(
		"mapTaskIdx %v: Beginning map task with config: %v.\n",
		mapTask.MapTaskIdx,
		mapTask,
	)

	// Open the map input file for reading.
	inputFile, err := os.Open(mapTask.MapperInputFileName)
	if err != nil {
		log.Fatalf("error opening mapper input file: %v\n", err)
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)

	// Open files for map output.
	outputManager := NewOutputManager(mapTask)
	defer outputManager.Close()

	util.Debug(
		"mapTaskIdx %v: Beginning mapping.\n", mapTask.MapTaskIdx,
	)

	// Get the mappingFunction to use.
	mappingFunction := mapTask.MappingFunction()

	for {
		// Read a line from the map input file.
		line, err := inputReader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("error reading file: %v\n", err)
		}

		// Apply the mapping function.
		mappingFunction(
			mapTask.MapperInputFileName,
			line,
			func(outputKeyValue types.KeyValue) {
				// Write the map outputs.
				outputManager.WriteKeyValue(outputKeyValue)
			},
		)
	}

	util.Debug(
		"mapTaskIdx %v: Completed map task.\n", mapTask.MapTaskIdx,
	)
}
