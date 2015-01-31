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

## Decoders
### Json Decoder

Reads JSON in message payload, and writes its keys and values to the Heka message's fields.

## Encoders
### Schema Librato Encoder
### Statmetric Segment Encoder

## Filters
### InfluxDB Batch Filter

## Outputs
### Postgres Output

Writes data to a Postgres database.

### Keen Output

Sends event data to Keen.io.

