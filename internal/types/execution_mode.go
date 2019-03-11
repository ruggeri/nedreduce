package types

type ExecutionMode string

const (
	Sequential  = ExecutionMode("sequential")
	Distributed = ExecutionMode("distributed")
)
