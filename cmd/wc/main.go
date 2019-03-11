package main

import (
	"fmt"
	"os"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		jobConfiguration := nedreduce.NewJobConfiguration(
			"wcseq",
			os.Args[3:],
			3,
			"WordSplittingMappingFunction",
			"WordCountingReducingFunction",
		)

		if os.Args[2] == "sequential" {
			nedreduce.RunSequentialJob(&jobConfiguration)
		} else {
			nedreduce.RunDistributedJob(&jobConfiguration, os.Args[2])
		}
	} else {
		nedreduce.RunWorker(
			os.Args[2],
			os.Args[3],
		)
	}
}
