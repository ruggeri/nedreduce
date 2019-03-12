#!/usr/bin/env sh

golint ./... | grep -vE "(underscore in package name)"
