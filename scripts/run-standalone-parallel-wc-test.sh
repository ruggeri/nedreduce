#!/bin/bash

set -e

# Used to wait until a server launched in the background is running.
# When a server is truly running, it will have opened a unix domain
# socket so that RPCs can be performed.
function wait_for_fname() {
  local fname=$1

  while [ ! -e $fname ]; do
    sleep 0.01
  done
}

# Start the coordinator
./build/bin/nedreduce run-coordinator coordinator &
wait_for_fname coordinator

# Starting workers.
./build/bin/nedreduce run-worker coordinator worker1 &
wait_for_fname worker1
./build/bin/nedreduce run-worker coordinator worker2 &
wait_for_fname worker2
./build/bin/nedreduce run-worker coordinator worker3 &
wait_for_fname worker3
./build/bin/nedreduce run-worker coordinator worker4 &
wait_for_fname worker4

# Submit the job to the coordinator. Wait for completion.
./build/bin/nedreduce submit-job coordinator assets/wc_distributed_job_config.json

# Start telling the workers to shut down.
./build/bin/nedreduce shutdown-worker worker1
./build/bin/nedreduce shutdown-worker worker2
./build/bin/nedreduce shutdown-worker worker3
./build/bin/nedreduce shutdown-worker worker4

# When all the workers have shut down, now shut down the coordinator.
./build/bin/nedreduce shutdown-coordinator coordinator

sort -n -k2 mrtmp.wc | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
