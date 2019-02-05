package mapreduce

import "io"

// GroupIterator is an iterator that yields successive KeyValues all in
// one key group.
type GroupIterator struct {
	GroupKey         string
	groupingIterator *GroupingIterator
}

// NewGroupIterator builds a GroupIterator...
func NewGroupIterator(groupingIterator *GroupingIterator) GroupIterator {
	return GroupIterator{
		GroupKey:         *groupingIterator.currentGroupKey,
		groupingIterator: groupingIterator,
	}
}

// Next yields the next KeyValue in the group, if any.
func (groupIterator *GroupIterator) Next() (*KeyValue, error) {
	keyValue, err := groupIterator.groupingIterator.advance()

	if err == io.EOF {
		// Group ended (either by new key or by end of input files).
		return nil, io.EOF
	}

	return keyValue, nil
}
