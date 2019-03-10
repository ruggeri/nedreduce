all: wc

./build/plugin.so: ./plugin/*
	go build -buildmode=plugin -o ./build/plugin.so ./plugin

wc: ./build/plugin.so
	go build -o ./build/bin/wc ./cmd/wc
