.PHONY: test
test:
	go test -race ./packets/ -v -count 1
	go test -race ./paho/ -v -count 1
