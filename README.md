heka-clever-plugins
===================

UnitTesting
-------

1. Check out [heka](https://github.com/mozilla-services/heka/) (or, at Clever, [heka-private](https://github.com/Clever/heka-private))

2. Add any new plugins or dependencies to heka's `cmake/plugin_loader.cmake`.

3. Build Heka as [described in the Heka docs](http://hekad.readthedocs.org/en/v0.6.0/installing.html).

4. Copy the modified source files of plugins you wish to test to `<hekadir>/build/heka/src/github.com/Clever`

5. Run `make test` to run all tests, or `ctest -R <test>` to run tests individually

### A note on mocks
Heka generates certain files in the pipeline package at compile-time. The `github.com/mozilla-services/heka/pipelinemock` package is generated at compile-time when following the above instructions. If you use vim-go or a similar plugin, it will not be able to locate the package.


Integration Testing
-------

### Testing go plguins:
After building your plugins into a new `heka-private` image (see the `UnitTesting` section above), you can test them in a local pipeline by using the [heka-testing](https://github.com/Clever/heka-testing) repo. 

### Testing lua plugins:
With [heka-testing](https://github.com/Clever/heka-testing), lua only plugins can be tested without even building out a new `heka-private` image. Just set the `HEKA_PLUGINS_SHA` to your latest commit. More details can be found in the `heka-testing` repo.


Plugins
-------

## Decoders
### Json Decoder

Reads JSON in message payload, and writes its keys and values to the Heka message's fields.

## Encoders
### Schema Librato Encoder
### Statmetric Segment Encoder
### Slack Encoder

Takes a Heka message and converts it to a form that can be sent to Slack, using `HttpOutput`.
Sends chat message to a Slack channel.

## Filters
### InfluxDB Batch Filter

## Outputs
### Postgres Output

Writes data to a Postgres database.

```toml
[ExamplePostgresOutput]
type = "PostgresOutput"

# Insert into this table in Postgres DB
insert_table = "test_table"

# insert_message_fields is a space delimited list of Heka Message Fields names.
# insert_table_columns is a space delimited list of Postgres table columns.
# It write those fields values in order into a INSERT INTO statement, i.e.
#   INSERT INTO "test_table" VALUES ($1 $2 $3)
# where $1 $2 $3 are values read from insert_fields
#
# `Timestamp` is a special case that reads the Heka message's timestamp.
# Otherwise, fields names correspond to Heka Message Fields.
insert_message_fields = "Timestamp field_a field_b"
insert_table_columns = "col_time col_a col_b"

# If true, will write NULL as a value for any missing field.
# If false, will error if any of insert_message_fields isn't present on the Heka message.
allow_missing_message_fields = false # default: true

# Database connection parameters
db_host = "localhost"
db_port = 5432
db_name = "name"
db_user = "user"
db_password = "password"
db_connection_timeout = 5

### Optional ###
# Inert into this schema in Postgres DB
insert_schema = "testschema"  # default: "public"
# Database connection parameters
db_ssl_mode = "disable" # default: "require"
db_connection_timeout = 5
db_max_open_connections = 1000

# Batching configuration
flush_interval = 1000 # max time before doing an insert (in milliseconds)
flush_count = 10000 # max number of messages to batch before inserting
```
### Firehose Output

Writes data to a [AWS Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/). Currently only supports pre-configured streams created in the aws console.

```
[ExampleFirehoseOutput]
type = "FirehoseOutput"

# The stream to write to
stream = "test_stream"

# The region the stream is in (a good guess is 'us-west-2')
region = 'us-west-2'
```
