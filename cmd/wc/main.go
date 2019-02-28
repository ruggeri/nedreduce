package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

func wordSplittingMappingFunction(
	filename string,
	line string,
	emitterFunction nedreduce.EmitterFunction,
) {
	words := strings.FieldsFunc(line, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	for _, word := range words {
		outputKeyValue := nedreduce.KeyValue{Key: word, Value: ""}
		emitterFunction(outputKeyValue)
	}
}

func wordCountingReducingFunction(
	groupKey string,
	groupIteratorFunction nedreduce.GroupIteratorFunction,
	emitterFunction nedreduce.EmitterFunction,
) {
	wordCount := 0

	for {
		_, err := groupIteratorFunction()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Unexpected error from group iterator")
		}

		wordCount++
	}

	keyValue := nedreduce.KeyValue{
		Key:   groupKey,
		Value: strconv.Itoa(wordCount),
	}

	emitterFunction(keyValue)
}

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
			wordSplittingMappingFunction,
			wordCountingReducingFunction,
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
			wordSplittingMappingFunction,
			wordCountingReducingFunction,
			100,
		)
	}
}
