#!/bin/bash

echo "==> Part I"
go test ./tests -run Sequential

echo ""
echo "==> Part II"
./scripts/test-wc.sh

echo ""
echo "==> Part III"
go test ./tests -run TestParallel

# TODO(HIGH): These tests don't pass yet.
echo ""
echo "==> Part IV"
go test ./tests -run Failure

# echo ""
# echo "==> Part V (inverted index)"
# (cd "$here" && sh ./test-ii.sh > /dev/null)
