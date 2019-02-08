package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	. "mapreduce/types"
	"os"
	"sort"
)

// MergeReducerOutputFiles combines the results of the many reduce jobs
// into a single output file.
//
// TODO: use merge sort. This is bogus.
func MergeReducerOutputFiles(jobName string, numReducers int) {
	Debug("Reducer output merge phase beginning...")

	kvs := make(map[string]string)
	for reduceTaskIdx := 0; reduceTaskIdx < numReducers; reduceTaskIdx++ {
		// Open reduce task output file. Setup decoder.
		reduceTaskOutputFileName := ReducerOutputFileName(
			jobName, reduceTaskIdx,
		)
		fmt.Printf("Merge: read %s\n", reduceTaskOutputFileName)
		reduceTaskOutputFile, err := os.Open(reduceTaskOutputFileName)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		defer reduceTaskOutputFile.Close()
		reduceTaskOutputDecoder := json.NewDecoder(reduceTaskOutputFile)

		// Read in entire file.
		for {
			var kv KeyValue
			err = reduceTaskOutputDecoder.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
	}

	// Sort the keys.
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Open merged output file.
	mergedOutputFile, err := os.Create("mrtmp." + jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	defer mergedOutputFile.Close()

	// Setup output writer.
	mergedOutputWriter := bufio.NewWriter(mergedOutputFile)
	// Remember to flush any buffered output at the end.
	defer mergedOutputWriter.Flush()

	// And write everything out.
	for _, k := range keys {
		fmt.Fprintf(mergedOutputWriter, "%s: %s\n", k, kvs[k])
	}
}
