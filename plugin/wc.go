package main

import (
	"io"
	"log"
	"strconv"
	"strings"
	"unicode"

	nedreduce "github.com/ruggeri/nedreduce/pkg"
)

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
