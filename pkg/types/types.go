package types

// An EmitterFunction is used by a MappingFunction or ReducingFunctino
// to emit KeyValues one at a time.
type EmitterFunction func(outputKeyValue KeyValue)

// A MappingFunction is the type of mapping function supplied by the
// user. It is called once per line of the input file. KeyValues are
// emitted one at a time using the `emitterFunction`.
type MappingFunction func(
	inputFilename string,
	line string,
	emitterFunction EmitterFunction,
)

// A GroupIteratorFunction is how a ReducingFunction is one-by-one
// passed the KeyValues that comprise a reduce group.
type GroupIteratorFunction func() (*KeyValue, error)

// A ReducingFunction is the type of function the user supplies to do
// the reducing. The ReducingFunction is called once per group. The user
// *must* iterate the group to the end using the supplied
// `groupIteratorFunction`. The user can emit KeyValues one-by-one using
// the `emitterFunction`.
type ReducingFunction func(
	groupKey string,
	groupIteratorFunction GroupIteratorFunction,
	emitterFunction EmitterFunction,
)
