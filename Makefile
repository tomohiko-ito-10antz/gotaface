.DEFAULT_GOAL:=help


.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m\n\t%s\n", $$1, $$2}'


.PHONY: lint
lint: ## lint
	find . -print | grep --regex '.*\.go' | xargs goimports -w
	go vet ./...


.PHONY: test
test: ## test
	go clean -testcache
	sh spanner/tools/test-schema.sh
	sh sqlite/tools/test-schema.sh
	go test ./...