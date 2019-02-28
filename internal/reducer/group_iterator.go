package reducer

import (
	"io"
	"log"

	. "github.com/ruggeri/nedreduce/pkg/types"
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
	if groupIterator.groupingIterator == nil {
		// As discussed below, groupingIterator is set to nil when the group
		// has been exhuasted.

		return nil, io.EOF
	}

	keyValue, err :=
		groupIterator.groupingIterator.advanceUnderlyingIterator()

	if err == io.EOF {
		// Group ended (either by new key or by end of input files).

		// I nil out the groupingIterator so that future calls to Next will
		// know *not* to advance the `groupingIterator`, and can instead
		// return `io.EOF` again.
		groupIterator.groupingIterator = nil

		return nil, io.EOF
	} else if err != nil {
		log.Fatalf("unexpected error in GroupIterator: %v\n", err)
	}

	return keyValue, nil
}

// Close is used to exhaust the GroupIterator. This is needed because we
// must advance the `GroupingIterator`s underlying iterator. We need to
// skip enough `KeyValues to get to the next group.
func (groupIterator *GroupIterator) Close() {
	for {
		_, err := groupIterator.Next()

		if err == io.EOF {
			break
		}
	}
}
