.PHONY: test unittest

unittest:
	go test -race -tags=unittest ./autopaho/ -v -count 1

test: unittest
	go test -race ./packets/ -v -count 1
	go test -race ./paho/ -v -count 1
	
