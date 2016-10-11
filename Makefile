.PHONY: lua-tests

test: lua-tests

lua-deps: # Install dependencies to run Lua tests
	apt-get update
	apt-get install lua5.1 luarocks
	luarocks install lua-cjson
	luarocks install busted

lua-tests: # Run tests for lua-based plugins
	@echo "Running tests for ./lua/filters"
	@pushd ./lua/filters; busted .; popd
	@echo ""
	@echo "Running tests for ./lua/decoders"
	@pushd ./lua/decoders; busted .; popd
