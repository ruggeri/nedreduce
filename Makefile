all: plugin wc

plugin:
	go build -buildmode=plugin -o ./build/plugin.so ./plugin

wc:
	go build -o ./build/bin/wc ./cmd/wc

.PHONY: plugin wc
