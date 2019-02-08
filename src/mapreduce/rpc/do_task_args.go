package rpc

import "mapreduce/common"

// DoTaskArgs holds the arguments that are passed to a Worker when a job
// is scheduled for it.
type DoTaskArgs struct {
	JobName  string
	JobPhase common.JobPhase
	// MapInputFileName will only be set if we are in the MapPhase.
	MapInputFileName string
	TaskIdx          int

	// NumTasksInOtherPhase is the total number of tasks in "other" phase;
	// mappers need this to compute the number of output bins, and
	// reducers needs this to know how many input files to collect.
	//
	// TODO: I personally consider this gross and lazy.
	NumTasksInOtherPhase int
}
