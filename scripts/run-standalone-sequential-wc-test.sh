#!/bin/bash

set -e

# Start the coordinator
./build/bin/wc run-coordinator coordinator &
COORDINATOR_PID=$!

# TODO: This is very lame. Basically, the coordinator is started above,
# but we need to wait until it is actually listening for RPCs. The
# correct way is that run-coordinator should spawn a process for running
# the coordinator, and *return* when the process has started listening.
sleep 1

# Submit the sequential job to the coordinator. Wait for completion.
./build/bin/wc submit-job coordinator sequential 3 assets/pg-*.txt

# TODO: I should kill the coordinator via RPC.
kill $COORDINATOR_PID

sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
