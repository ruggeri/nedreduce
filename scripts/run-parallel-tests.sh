#!/bin/bash

echo ""
echo "==> Part IIIa"
go test -v ./tests -run TestParallelBasic

echo ""
echo "==> Part IIIb"
go test -v ./tests -run TestParallelCheck

echo ""
echo "==> Part IV"
go test -v ./tests -run Failure

# echo ""
# echo "==> Part V (inverted index)"
# (cd "$here" && sh ./test-ii.sh > /dev/null)
