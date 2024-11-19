.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...
	echo
	echo "Running integration tests..."
	echo
	cd specgen/specgen/tests/parse_specs/ && go test $(GOTEST_FLAGS) -race ./...
	cd specgen/specgen/tests/write_and_combine/ && go test $(GOTEST_FLAGS) -race ./...


.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
