package reducer

import (
	"encoding/json"
	"io"
	"log"
	"mapreduce/common"
)

// MergedInputIterator merges many sorted files. It peeks only one value
// ahead in the decoders.
type MergedInputIterator struct {
	inputDecoders      []*json.Decoder
	nextInputKeyValues []*common.KeyValue
}

// NewMergedInputIterator builds the MergedInputIterator and peeks each
// decoder.
func NewMergedInputIterator(inputDecoders []*json.Decoder) MergedInputIterator {
	numReducers := len(inputDecoders)
	iterator := MergedInputIterator{
		inputDecoders:      inputDecoders,
		nextInputKeyValues: make([]*common.KeyValue, numReducers),
	}

	for idx := 0; idx < numReducers; idx++ {
		iterator.pull(idx)
	}

	return iterator
}

// pull pulls the next KeyValue from the specified decoder.
func (mergedInputIterator *MergedInputIterator) pull(idx int) {
	inputDecoder := mergedInputIterator.inputDecoders[idx]

	if inputDecoder == nil {
		// We have previously exhausted this decoder.
		return
	}

	// Try to decode a KeyValue
	inputKeyValue := &common.KeyValue{}
	err := inputDecoder.Decode(inputKeyValue)

	// If there was an error, the log and die.
	if err != nil && err != io.EOF {
		log.Fatalf("error decoding reducer input: %v\n", err)
	}

	if err == io.EOF {
		// If we have hit the end of this file, then mark it as exhausted
		mergedInputIterator.inputDecoders[idx] = nil
		mergedInputIterator.nextInputKeyValues[idx] = nil
	} else {
		// Otherwise, update the peeked value.
		mergedInputIterator.nextInputKeyValues[idx] = inputKeyValue
	}
}

// Next picks the smallest value amongst the peeked values, and replaces
// it with a newly peeked value.
func (mergedInputIterator *MergedInputIterator) Next() (*common.KeyValue, error) {
	leastInputKeyValueIdx := -1
	var leastInputKeyValue *common.KeyValue

	// Scan for the least input with the least key.
	for inputIdx, inputKeyValue := range mergedInputIterator.nextInputKeyValues {
		if inputKeyValue == nil {
			// Skip over exhausted input files.
			continue
		}

		if leastInputKeyValue == nil {
			// If this is our first KeyValue encountered, it is by default the
			// least.
			leastInputKeyValueIdx, leastInputKeyValue = inputIdx, inputKeyValue
		} else if inputKeyValue.Key < leastInputKeyValue.Key {
			// Else, we must compare to see if we have found a lesser key.
			leastInputKeyValueIdx, leastInputKeyValue = inputIdx, inputKeyValue
		}
	}

	if leastInputKeyValueIdx == -1 {
		// If everyone was exhausted, then this is the end!
		return nil, io.EOF
	}

	// Else, we're going to have to pull a new peeked value for next time.
	mergedInputIterator.pull(leastInputKeyValueIdx)

	return leastInputKeyValue, nil
}
