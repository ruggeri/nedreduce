package types

// JobPhase is an enum for either MapPhase or ReducerPhase.
type JobPhase string

const (
	// MapPhase means we are in the map phase (duh).
	MapPhase JobPhase = "MapPhase"
	// ReducePhase means we are in the reduce phase (duh).
	ReducePhase = "ReducePhase"
)
