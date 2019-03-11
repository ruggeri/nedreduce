#!/bin/bash

set -e

# Start the coordinator
./build/bin/wc run-coordinator coordinator &

# TODO: This is very lame. Basically, the coordinator is started above,
# but we need to wait until it is actually listening for RPCs. The
# correct way is that run-coordinator should spawn a process for running
# the coordinator, and *return* when the process has started listening.
sleep 0.01

# Starting workers.
./build/bin/wc run-worker coordinator worker1 &
./build/bin/wc run-worker coordinator worker2 &
./build/bin/wc run-worker coordinator worker3 &
./build/bin/wc run-worker coordinator worker4 &
# TODO: Again, also lame.
sleep 0.01

# Submit the job to the coordinator. Wait for completion.
./build/bin/wc submit-job coordinator distributed 3 assets/pg-*.txt

# Having waited for the cluster to finish the job, now tell the
# coordinator to shut down.
./build/bin/wc shutdown-coordinator coordinator

# Tell the workers to shutdown, too.
./build/bin/wc run-worker coordinator worker1
./build/bin/wc run-worker coordinator worker2
./build/bin/wc run-worker coordinator worker3
./build/bin/wc run-worker coordinator worker4

sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
