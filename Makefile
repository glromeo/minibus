APP=minibus

build:
	go build -o bin/$(APP) ./cmd/minibus

run:
	go run ./cmd/minibus

test:
	go test ./...

fmt:
	go fmt ./...