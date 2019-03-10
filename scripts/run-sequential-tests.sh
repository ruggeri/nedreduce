#!/bin/bash

set -e

echo "==> Part I"
go test -v ./tests -run Sequential
