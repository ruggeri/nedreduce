package reducer

import (
	"encoding/json"
	"io"
	"log"

	"github.com/ruggeri/nedreduce/internal/types"
)

// A GroupingIterator is an iterator that yields GroupIterators.
type GroupingIterator struct {
	currentGroupKey     *string
	peekedKeyValue      *types.KeyValue
	mergedInputIterator MergedInputIterator
}

// NewGroupingIterator gets ready to produce the next GroupIterator.
func NewGroupingIterator(
	inputDecoders []*json.Decoder,
) GroupingIterator {
	iter := GroupingIterator{
		currentGroupKey:     nil,
		mergedInputIterator: NewMergedInputIterator(inputDecoders),
	}

	// Peek the first KeyValue.
	peekedKeyValue, err := iter.mergedInputIterator.Next()
	if err != nil {
		log.Panicf(
			"expected at least one KeyValue but got error: %v\n", err,
		)
	}

	// Get ready to handle the next group.
	iter.currentGroupKey = &peekedKeyValue.Key
	iter.peekedKeyValue = peekedKeyValue

	return iter
}

// advanceUnderlyingIterator is called by GroupIterator. It keeps
// handing KeyValues to the GroupIterator until either (1) the group
// ends because a new key is encountered, or (2) the group ends because
// the input is exhausted.
func (iter *GroupingIterator) advanceUnderlyingIterator() (*types.KeyValue, error) {
	if iter.peekedKeyValue == nil {
		// We ran out of KeyValues; the input files must be exhausted. This
		// is the end of the group, and there are no future groups.
		iter.currentGroupKey = nil
		return nil, io.EOF
	} else if *(iter.currentGroupKey) != iter.peekedKeyValue.Key {
		// This is the end of the current group. Update the currentGroupKey
		// so that, for the next GroupIterator, peekedKeyValue will be the
		// first yielded KeyValue of the new group.
		iter.currentGroupKey = &iter.peekedKeyValue.Key
		return nil, io.EOF
	} else {
		// Else, we will yield peekedKeyValue as the next KeyValue in the
		// group.
		nextKeyValueForCurrentGroup := iter.peekedKeyValue

		// We will need to "advance" the peekedKeyValue.
		peekedKeyValue, err := iter.mergedInputIterator.Next()
		if err == io.EOF {
			// We have exhausted the input so there is no KeyValue to peek.
			iter.peekedKeyValue = nil
		} else if err != nil {
			log.Panicf("unexpected grouping error: %v\n", err)
		} else {
			iter.peekedKeyValue = peekedKeyValue
		}

		return nextKeyValueForCurrentGroup, nil
	}
}

// Next iterates over GroupIterators.
func (iter *GroupingIterator) Next() (*GroupIterator, error) {
	if iter.currentGroupKey == nil {
		// We have exhausted all groups!
		return nil, io.EOF
	}

	groupIterator := NewGroupIterator(iter)
	return &groupIterator, nil
}
