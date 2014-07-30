heka-clever-plugins
===================

Testing
-------

1. Check out [heka](https://github.com/mozilla-services/heka/) (or, at Clever, [heka-private](https://github.com/Clever/heka-private))

2. Add any new plugins or dependencies to heka's `cmake/plugin_loader.cmake`.

3. Build Heka as [described in the Heka docs](http://hekad.readthedocs.org/en/v0.6.0/installing.html).

4. Copy the modified source files of plugins you wish to test to `<hekadir>/build/heka/src/github.com/Clever`

5. Run `make test` to run all tests, or `ctest -R <test>` to run tests individually

Plugins
-------

### keen-output.go
Sends event data to Keen.io.
TODO: Note that we may wish to replace this with the built-in HttpOutput plugin (see https://github.com/mozilla-services/heka/pull/941),
which unexpectedly made it into Heka's 0.6 release.
