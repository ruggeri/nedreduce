#!/bin/bash

./build/bin/wc coordinator sequential assets/pg-*.txt
sort -n -k2 mrtmp.wcseq | tail -10

# TODO(LOW): Restore test so that it does a comparison to correct
# counts.
