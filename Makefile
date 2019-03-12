all: plugin nedreduce

plugin:
	go build -buildmode=plugin -o ./build/plugin.so ./plugin

nedreduce:
	go build -o ./build/bin/nedreduce ./cmd/nedreduce

.PHONY: plugin nedreduce
