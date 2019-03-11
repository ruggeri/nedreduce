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

# Starting workers.
./build/bin/wc run-worker coordinator worker1 assets/pg-*.txt &
WORKER1_PID=$!
./build/bin/wc run-worker coordinator worker2 assets/pg-*.txt &
WORKER2_PID=$!
./build/bin/wc run-worker coordinator worker3 assets/pg-*.txt &
WORKER3_PID=$!
./build/bin/wc run-worker coordinator worker4 assets/pg-*.txt &
WORKER4_PID=$!

# Submit the job to the coordinator. Wait for completion.
./build/bin/wc submit-job coordinator distributed 3 assets/pg-*.txt

# Having waited for the coordinator to finish the job, now tell it to
# shut down.
./build/bin/wc shutdown-coordinator coordinator

# TODO: We really should kill the WORKER_PIDs properly
echo $WORKER1_PID
echo $WORKER2_PID
echo $WORKER3_PID
echo $WORKER4_PID

sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
