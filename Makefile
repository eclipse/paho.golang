.PHONY: test unittest

unittest:
	go test -coverprofile /tmp/autopaho_coverage.out -race -tags=unittest ./autopaho/ -v -count 1

test: unittest
	go test -coverprofile /tmp/packets_coverage.out -race ./packets/ -v -count 1
	go test -coverprofile /tmp/paho_coverage.out -race ./paho/ -v -count 1

cover:
	go tool cover -func=/tmp/autopaho_coverage.out
	go tool cover -func=/tmp/packets_coverage.out
	go tool cover -func=/tmp/paho_coverage.out

cover_browser:
	go tool cover -html=/tmp/autopaho_coverage.out
	go tool cover -html=/tmp/packets_coverage.out
	go tool cover -html=/tmp/paho_coverage.out

build_chat:
	go build -o ./bin/chat ./paho/cmd/chat

build_rpc:
	go build -o ./bin/rpc ./paho/cmd/rpc

build_rpc_cm:
	go build -o ./bin/rpc_auto ./autopaho/cmd/rpc

build_pub:
	go build -o ./bin/stdinpub ./paho/cmd/stdinpub

build_sub:
	go build -o ./bin/stdoutsub ./paho/cmd/stdoutsub

build: build_chat build_rpc build_pub build_sub build_rpc_cm