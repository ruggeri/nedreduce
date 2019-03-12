#!/usr/bin/env bash

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

# Start the coordinator.
./build/bin/wc run-coordinator coordinator &
wait_for_fname coordinator

# Submit the sequential job to the coordinator. Wait for completion.
./build/bin/wc submit-job coordinator sequential 3 assets/pg-*.txt

# Having waited for the coordinator to finish the job, now tell it to
# shut down.
./build/bin/wc shutdown-coordinator coordinator

sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
