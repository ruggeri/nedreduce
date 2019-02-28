package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"

	. "github.com/ruggeri/nedreduce/pkg/types"
)

// MergeReducerOutputFiles combines the results of the many reduce jobs
// into a single output file. Presently a `Master` runs this function at
// the end of a job, but it is typical for real-world MR jobs not to do
// that. Producing many output files is a feature, not a bug.
//
// Anyway, I feel like this function is unnecessary: if you wanted to
// concatenate all partitions you could just write another MR job with a
// single reducer... That would be the right thing to do if reducer
// output files lived on separate machines and weren't otherwise
// remotely accessible.
//
// Anyway, I leave this here as a concession to the way the tests are
// currently run.
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
