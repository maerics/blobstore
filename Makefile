build: test
	go build -o ./blobstore ./cmd

test: fmt vet
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...
