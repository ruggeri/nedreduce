#!/bin/bash

./build/bin/wc master sequential assets/pg-*.txt
sort -n -k2 mrtmp.wcseq | tail -10
