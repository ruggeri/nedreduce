package types

// An EmitterFunction is used by a MappingFunction or ReducingFunctino
// to emit KeyValues.
type EmitterFunction func(outputKeyValue KeyValue)

// A MappingFunction is the type of mapping function supplied by the
// user.
type MappingFunction func(
	filename string,
	line string,
	mappingEmitterFunction EmitterFunction,
)

// A GroupIteratorFunction is how a ReducingFunction is one-by-one
// passed the KeyValues that comprise a reduce group.
type GroupIteratorFunction func() (*KeyValue, error)

// A ReducingFunction is the type of function the user supplies to do
// the reducing.
type ReducingFunction func(
	groupKey string,
	groupIteratorFunction GroupIteratorFunction,
	reducingEmitterFunction EmitterFunction,
)
