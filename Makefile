VERSION := $(shell git rev-parse --short HEAD)
BUILDTIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

GOLDFLAGS += -s -w
GOLDFLAGS += -X main.Version=$(VERSION)
GOLDFLAGS += -X main.Buildtime=$(BUILDTIME)
GOFLAGS = -ldflags "$(GOLDFLAGS)"

.PHONY: all deps tidy build build-all release clean

all: deps tidy build

deps:
	go mod download

tidy:
	go mod tidy

lint:
	golangci-lint config path
	golangci-lint config verify
	golangci-lint run

build:
	goreleaser build --clean --snapshot

build-local:
	goreleaser build --clean --snapshot --single-target --id ec2-ssh

build-all: build

optimize:
	if [ -x /usr/bin/upx ] || [ -x /usr/local/bin/upx ]; then upx --brute ${BINARY_NAME}-*; fi

release:
	goreleaser release --clean

test:
	go test -v ./...

clean:
	go clean
	rm -rf dist bin

