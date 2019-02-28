all: wc

wc:
	go build -o ./build/bin/wc ./cmd/wc
