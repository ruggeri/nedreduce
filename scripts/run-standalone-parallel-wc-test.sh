#!/bin/bash

./build/bin/wc master localhost:3000 assets/pg-*.txt &
MASTER_PID=$!

./build/bin/wc worker localhost:3000 localhost:3001 assets/pg-*.txt &
./build/bin/wc worker localhost:3000 localhost:3002 assets/pg-*.txt &

wait $MASTER_PID

sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
