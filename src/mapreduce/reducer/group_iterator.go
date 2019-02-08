package reducer

import (
	"io"
	"log"
	. "mapreduce/types"
)

// A GroupIterator is an iterator that yields successive KeyValues all
// in one key group.
type GroupIterator struct {
	GroupKey         string
	groupingIterator *GroupingIterator
}

// NewGroupIterator builds a GroupIterator...
func NewGroupIterator(
	groupingIterator *GroupingIterator,
) GroupIterator {
	return GroupIterator{
		GroupKey:         *groupingIterator.currentGroupKey,
		groupingIterator: groupingIterator,
	}
}

// Next yields the next KeyValue in the group, if any.
func (groupIterator *GroupIterator) Next() (*KeyValue, error) {
	// TODO: It's dangerous to call Next after receiving io.EOF (since
	// this will affect the groupingIterator). I should prolly make that a
	// no-op...
	keyValue, err :=
		groupIterator.groupingIterator.advanceUnderlyingIterator()

	if err == io.EOF {
		// Group ended (either by new key or by end of input files).
		return nil, io.EOF
	} else if err != nil {
		log.Fatalf("unexpected error in GroupIterator: %v\n", err)
	}

	return keyValue, nil
}
