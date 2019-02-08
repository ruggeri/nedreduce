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
func ExecuteMapping(configuration Configuration) {
	common.Debug(
		"mapTaskIdx %v: Beginning map task with config: %v.\n",
		configuration.MapTaskIdx,
		configuration,
	)

	// Open the map input file for reading.
	inputFile, err := os.Open(configuration.MapperInputFileName)
	if err != nil {
		log.Fatalf("error opening mapper input file: %v\n", err)
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)

	// Open files for map output.
	outputManager := NewOutputManager(configuration)
	defer outputManager.Close()

	common.Debug("mapTaskIdx %v: Beginning mapping.\n", configuration.MapTaskIdx)
	for {
		// Read a line from the map input file.
		line, err := inputReader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("error reading file: %v\n", err)
		}

		// Apply the mapping function.
		configuration.MappingFunction(
			configuration.MapperInputFileName,
			line,
			func(outputKeyValue common.KeyValue) {
				// Write the map outputs.
				outputManager.WriteKeyValue(outputKeyValue)
			},
		)
	}

	common.Debug("mapTaskIdx %v: Completed map task.\n", configuration.MapTaskIdx)
}
