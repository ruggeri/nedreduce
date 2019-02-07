package mapper

import (
	"bufio"
	"io"
	"log"
	"mapreduce/common"
	"os"
)

// MappingEmitterFunction is used by a MappingFunction to emit
// KeyValues.
type MappingEmitterFunction func(outputKeyValue common.KeyValue)

// A MappingFunction is the type of mapping function supplied by the
// user.
type MappingFunction func(filename string, line string, mappingEmitterFunction MappingEmitterFunction)

// ExecuteMapping runs a map task.
func ExecuteMapping(
	jobName string, // the name of the MapReduce job
	mapTaskIdx int, // which map task this is
	inputFileName string,
	numReducers int, // the number of reduce task that will be run ("R" in the paper)
	mappingFunction MappingFunction,
) {
	// Open the map input file for reading.
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("error opening mapper input file: %v\n", err)
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)

	// Open files for map output.
	mapOutputManager := NewMapOutputManager(jobName, mapTaskIdx, numReducers)
	defer mapOutputManager.Close()

	for {
		// Read a line from the map input file.
		line, err := inputReader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("error reading file: %v\n", err)
		}

		// Apply the mapping function.
		mappingFunction(inputFileName, line, func(outputKeyValue common.KeyValue) {
			// Write the map outputs.
			mapOutputManager.WriteKeyValue(outputKeyValue)
		})
	}
}
