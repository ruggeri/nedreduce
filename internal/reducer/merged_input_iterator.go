package reducer

import (
	"encoding/json"
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/types"
)

// A MergedInputIterator merges many sorted files. It peeks only one
// value ahead in each of the decoders.
type MergedInputIterator struct {
	inputDecoders        []*json.Decoder
	peekedInputKeyValues []*types.KeyValue
}

// NewMergedInputIterator builds the MergedInputIterator and peeks each
// decoder.
func NewMergedInputIterator(
	inputDecoders []*json.Decoder,
) MergedInputIterator {
	numReducers := len(inputDecoders)
	iterator := MergedInputIterator{
		inputDecoders:        inputDecoders,
		peekedInputKeyValues: make([]*types.KeyValue, numReducers),
	}

	for idx := 0; idx < numReducers; idx++ {
		iterator.pullNextPeekedValue(idx)
	}

	return iterator
}

// pullNextPeekedValue pulls the next KeyValue from the specified
// decoder and sets it in peekedInputKeyValues.
func (mergedInputIterator *MergedInputIterator) pullNextPeekedValue(
	inputDecoderIdx int,
) {
	inputDecoder := mergedInputIterator.inputDecoders[inputDecoderIdx]

	if inputDecoder == nil {
		// We have previously exhausted this decoder.
		return
	}

	// Try to decode a KeyValue.
	inputKeyValue := &types.KeyValue{}
	err := inputDecoder.Decode(inputKeyValue)

	// If there was an error, then log and die.
	if err != nil && err != io.EOF {
		log.Fatalf("error decoding reducer input: %v\n", err)
	}

	if err == io.EOF {
		// If we have hit the end of this file, then mark this decoder as
		// exhausted.
		mergedInputIterator.inputDecoders[inputDecoderIdx] = nil
		mergedInputIterator.peekedInputKeyValues[inputDecoderIdx] = nil
	} else {
		// Otherwise, update the peeked value.
		mergedInputIterator.peekedInputKeyValues[inputDecoderIdx] = inputKeyValue
	}
}

// Next picks the smallest value amongst the peeked values, and replaces
// it with a newly peeked value.
func (mergedInputIterator *MergedInputIterator) Next() (*types.KeyValue, error) {
	leastInputKeyValueIdx := -1
	var leastInputKeyValue *types.KeyValue

	// Scan for the least input with the least key.
	//
	// TODO(LOW): If there were lots of map tasks, it might be efficient to use
	// a min heap here.
	for inputIdx, inputKeyValue := range mergedInputIterator.peekedInputKeyValues {
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
	mergedInputIterator.pullNextPeekedValue(leastInputKeyValueIdx)

	return leastInputKeyValue, nil
}
