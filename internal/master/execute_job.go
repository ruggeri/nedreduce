package master

import (
	"fmt"

	"github.com/ruggeri/nedreduce/internal/util"
)

// executeJob executes a mapreduce job.
func (master *Master) executeJob(
	runMapPhase func(*Master),
	runReducePhase func(*Master),
) {
	jobConfiguration := master.jobConfiguration

	fmt.Printf("%s: Starting Map/Reduce task %s\n", master.address, jobConfiguration.JobName)

	fmt.Printf("%s: Beginning map phase\n", master.address)
	runMapPhase(master)
	fmt.Printf("%s: Map phase completed\n", master.address)

	fmt.Printf("%s: Beginning reduce phase\n", master.address)
	runReducePhase(master)
	fmt.Printf("%s: Reduce phase completed\n", master.address)

	fmt.Printf("%s: Beginning final files cleanup\n", master.address)
	util.MergeReducerOutputFiles(jobConfiguration.JobName, jobConfiguration.NumReducers)
	fmt.Printf("%s: Completed stats collection and files cleanup\n", master.address)

	fmt.Printf("%s: Map/Reduce task completed\n", master.address)

	master.Shutdown()
}
