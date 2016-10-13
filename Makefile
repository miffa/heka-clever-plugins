.PHONY: lua-tests lua-deps

SHELL := /bin/bash
OS=$(shell uname | tr A-Z a-z)
ifeq ($(OS),darwin)
LUAROCKS := luarocks-5.1
else
LUAROCKS := luarocks
endif

test: lua-tests

lua-deps: # Install dependencies to run Lua tests
	@echo "Installing Lua"
	if [ "$(OS)" == "darwin" ]; then brew install lua51; else apt-get update && apt-get install lua5.1 luarocks; fi
	@echo "Installing Luarocks"
	$(LUAROCKS) install lua-cjson
	$(LUAROCKS) install busted

lua-tests: # Run tests for lua-based plugins
	@echo "Running tests for ./lua/filters"
	@pushd ./lua/filters; busted .; popd
	@echo ""
	@echo "Running tests for ./lua/decoders"
	@pushd ./lua/decoders; busted .; popd
