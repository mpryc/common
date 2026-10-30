# SHELL defines bash so all the inline scripts here will work as expected.
SHELL := /bin/bash

# GO_VERSION refers to the version of Golang to be downloaded when running dockerized version
GO_VERSION = 1.20
GOIMPORTS_VERSION = v0.11.0
SORT_IMPORTS_VERSION = v0.2.1

# Run go in a container
# --rm                                                          = remove container when stopped
# -v $$(pwd):/home/go/src/github.com/medik8s/node-maintenance-operator = bind mount current dir in container
# -u $$(id -u)                                                  = use current user (else new / modified files will be owned by root)
# -w /home/go/src/github.com/medik8s/node-maintenance-operator         = working dir
# -e ...                                                        = some env vars, especially set cache to a user writable dir
# --entrypoint /bin bash ... -c                                 = run bash -c on start; that means the actual command(s) need be wrapped in double quotes, see e.g. check target which will run: bash -c "make test"
export DOCKER_GO=docker run --rm -v $$(pwd):/home/go/src/github.com/medik8s/common \
	-u $$(id -u) -w /home/go/src/github.com/medik8s/common \
	-e "GOPATH=/go" -e "XDG_CACHE_HOME=/tmp/.cache" \
	--entrypoint /bin/bash golang:$(GO_VERSION) -c

# go-install-tool will 'go install' any package $2 and install it to $1.
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
define go-install-tool
@[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
GOBIN=$(PROJECT_DIR)/bin GOFLAGS='' go install $(2) ;\
rm -rf $$TMP_DIR ;\
}
endef

.PHONY: build
build: ## Build.
	go build ./...

.PHONY: test
test: ## Run unit tests.
	go test ./... -coverprofile cover.out -v

.PHONY: lint
lint: tidy goimports fix-imports vet verify-nodestate-deps verify-no-changes ## Run linters etc.

.PHONY: verify-nodestate-deps
verify-nodestate-deps: ## Verify pkg/nodestate imports only the standard library.
	./hack/verify-nodestate-deps.sh

.PHONY: check
check: ## Dockerized version of make test with additional verifications
	$(DOCKER_GO) "make lint test"

.PHONY: goimports
goimports: install-goimports ## Run go fmt against code.
	$(GOIMPORTS) -w ./pkg

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: verify-no-changes
verify-no-changes: ## verify there are no un-staged changes
	./hack/verify-diff.sh

.PHONY: tidy
tidy: ## Runs go mod tidy
	go mod tidy

SORT_IMPORTS = $(shell pwd)/bin/sort-imports
.PHONY: sort-imports
sort-imports: ## Download sort-imports locally if necessary.
	$(call go-install-tool,$(SORT_IMPORTS),github.com/slintes/sort-imports@$(SORT_IMPORTS_VERSION))

.PHONY: test-imports
test-imports: sort-imports ## Check for sorted imports
	$(SORT_IMPORTS) .

.PHONY: fix-imports
fix-imports: sort-imports ## Sort imports
	$(SORT_IMPORTS) -w .

GOIMPORTS = $(shell pwd)/bin/goimports
.PHONY: install-goimports
install-goimports: ## updates goimports.
	$(call go-install-tool,$(GOIMPORTS),golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION))
