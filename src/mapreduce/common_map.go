package mapreduce

import (
	"bufio"
	"io"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// Output map input file for reading.
	inputFile, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("error opening mapper input file: %v\n", err)
	}
	defer inputFile.Close()
	inputReader := bufio.NewReader(inputFile)

	// Open files for map output.
	mapOutputManager := NewMapOutputManager(jobName, mapTask, nReduce)
	defer mapOutputManager.Close()

	for {
		// Read a line from the map input file.
		line, err := inputReader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error reading file: %v\n", err)
		}

		// Apply the map function (I wish this gave us an iterator...).
		keyValues := mapF(inFile, line)

		// Write the map outputs.
		for _, keyValue := range keyValues {
			mapOutputManager.WriteKeyValue(keyValue)
		}
	}
}
