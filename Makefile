test: fmt vet
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...
