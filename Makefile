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

.PHONY: tidy-all
tidy-all:
	@echo "Tidying up module in parse_specs directory"
	@(cd specgen/specgen/tests/parse_specs && go mod tidy)
	@echo "Tidying up subdirectories..."
	@for dir in specgen/specgen/tests/parse_specs/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Processing directory: $$dir"; \
			(cd "$$dir" && go mod tidy) || exit 1; \
		fi \
	done

	@echo "Tidying up module in write_and_combine directory"
	@(cd specgen/specgen/tests/write_and_combine && go mod tidy)
	@echo "Tidying up subdirectories..."
	@for dir in specgen/specgen/tests/write_and_combine/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Processing directory: $$dir"; \
			(cd "$$dir" && go mod tidy) || exit 1; \
		fi \
	done
	@echo "Module tidying complete."
