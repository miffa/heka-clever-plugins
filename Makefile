.PHONY: lua-tests lua-deps

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

test: lua-tests

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
