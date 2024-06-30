.PHONY: help
help: ## Shows help messages.
	@grep -E '^[0-9a-zA-Z_-]+:(.*?## .*)?$$' $(MAKEFILE_LIST) | sed 's/^Makefile://' | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: dependencies
dependencies: ## Install all dependencies for build and unit_test
	@go install mvdan.cc/gofumpt@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install github.com/vektra/mockery/v2@latest
	@go mod tidy


.PHONY: lint
lint:
	gofumpt -w -l .
	golangci-lint run ./...


.PHONY: mocks
mocks:
	@go generate ./...


.PHONY: coverage
coverage:
	@go test $$(go list ./... | grep -v mocks) -cover
