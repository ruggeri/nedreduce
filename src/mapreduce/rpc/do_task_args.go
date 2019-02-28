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

	// TODO: Right now Workers have Mapping and ReducingFunctions
	// "hard-coded" when they are instantiated. I think a more realistic
	// implementation would have the `Master` tell a Worker what functions
	// it will using for this job.
}
