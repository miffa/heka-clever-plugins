include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: lua-tests lua-deps go-deps go-tests test

# Lua setup
SHELL := /bin/bash
OS=$(shell uname | tr A-Z a-z)
ifeq ($(OS),darwin)
LUA := /usr/local/bin/lua5.1
LUAROCKS := /usr/local/bin/luarocks-5.1
ROCKSDIR := /usr/local/lib/luarocks/rocks-5.1/
else
LUA := /usr/bin/lua5.1
LUAROCKS := /usr/bin/luarocks
ROCKSDIR := /usr/local/lib/luarocks/rocks/
endif

# Go setup -- only run "postgres" tests for now
PKGS = $(shell GO15VENDOREXPERIMENT=1 go list ./... | grep -v "vendor/" | grep -v "db" | grep "postgres")

test: lua-tests go-tests

lua-deps: $(LUA) $(ROCKSDIR)/lua-cjson $(ROCKSDIR)/busted # Install dependencies to run Lua tests

$(LUA):
	@echo "Installing Lua"
	if [ "$(OS)" == "darwin" ]; then brew install lua51; else apt-get update && apt-get install lua5.1 luarocks; fi

$(ROCKSDIR)/lua-cjson:
	$(LUAROCKS) install lua-cjson

$(ROCKSDIR)/busted:
	$(LUAROCKS) install busted

lua-tests: lua-deps # Run tests for lua-based plugins
	@echo "Running tests for ./lua/filters"
	@pushd ./lua/filters; busted . && popd
	@echo ""
	@echo "Running tests for ./lua/decoders"
	@pushd ./lua/decoders; busted . && popd
	@echo ""
	@echo "Running tests for ./lua/encoders"
	@pushd ./lua/encoders; busted . && popd

$(GOPATH)/bin/glide:
	@go get github.com/Masterminds/glide

# install go deps with glide
go-deps: $(GOPATH)/bin/glide
	@$(GOPATH)/bin/glide install

go-tests: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)
