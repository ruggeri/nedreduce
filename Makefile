all: plugin nedreduce

# NB: the -race flag is super cool!

plugin:
	go build -buildmode=plugin -o ./build/plugin.so ./plugin

nedreduce:
	go build -o ./build/bin/nedreduce ./cmd/nedreduce

.PHONY: plugin nedreduce
