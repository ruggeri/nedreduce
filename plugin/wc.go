package main

// This is the "plugin" code which is dynamically loaded by workers so
// that they can load client code.
//
// This lets you start workers up even before you ship the code you want
// to run. When you run a job, you deploy the plugin code, *not* the
// JobCoordinator or Worker code.
//
// That is, you don't need to recompile and redeploy the nedreduce code
// every time you simply want to deploy a new nedreduce job.

import (
	"io"
	"log"
	"strconv"
	"strings"
	"unicode"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

// WordSplittingMappingFunction splits each line of text into a series
// of words.
func WordSplittingMappingFunction(
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

// WordSplittingMappingFunctionForTest splits each line of text into a
// series of words, but doesn't bother to strip out non-letter
// characters.
func WordSplittingMappingFunctionForTest(
	filename string,
	line string,
	emitterFunction nedreduce.EmitterFunction,
) {
	words := strings.Fields(line)

	for _, word := range words {
		outputKeyValue := nedreduce.KeyValue{Key: word, Value: ""}
		emitterFunction(outputKeyValue)
	}
}

// WordCountingReducingFunction counts the number of records in a group.
// It counts how many occurrences of the word were encountered by the
// mappers.
func WordCountingReducingFunction(
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
			log.Panicf("Unexpected error from group iterator")
		}

		wordCount++
	}

	keyValue := nedreduce.KeyValue{
		Key:   groupKey,
		Value: strconv.Itoa(wordCount),
	}

	emitterFunction(keyValue)
}
