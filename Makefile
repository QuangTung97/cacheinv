.PHONY: all lint install-tools test test-race coverage

all: lint test test-race coverage

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive

test:
	go test -p 1 -count=1 -covermode=count -coverprofile=coverage.out ./...

test-race:
	go test -p 1 -race -count=1 ./...

coverage:
	go tool cover -func coverage.out | grep ^total
