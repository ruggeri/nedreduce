package common

// JobPhase is an enum for either MapPhase or ReducerPhase.
type JobPhase string

const (
	// MapPhase means we are in the map phase (duh).
	MapPhase JobPhase = "mapPhase"
	// ReducePhase means we are in the reduce phase (duh).
	ReducePhase = "reducePhase"
)
