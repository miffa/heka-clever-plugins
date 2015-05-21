package heka_clever_plugins

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Clever/heka-clever-plugins/postgres"
	_ "github.com/lib/pq"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
)

type PostgresOutput struct {
	db                        *postgres.PostgresDB
	insertSchema              string
	insertTable               string
	insertMessageFields       []string
	insertTableColumns        []string
	flushInterval             uint32
	flushCount                int // Max messages before flush
	allowMissingMessageFields bool
}

type PostgresOutputConfig struct {
	// Table name and colums. Message fields to write.
	InsertSchema        string `toml:"insert_schema"`
	InsertTable         string `toml:"insert_table"`
	InsertTableColumns  string `toml:"insert_table_columns"`
	InsertMessageFields string `toml:"insert_message_fields"`
	// If a field is missing in the Heka message, allow writing NULL
	AllowMissingMessageFields bool `toml:"allow_missing_message_fields"`

	// Database Connection
	DBHost               string `toml:"db_host"`
	DBPort               int    `toml:"db_port"`
	DBName               string `toml:"db_name"`
	DBUser               string `toml:"db_user"`
	DBPassword           string `toml:"db_password"`
	DBConnectionTimeout  int    `toml:"db_connection_timeout"`
	DBMaxOpenConnections int    `toml:"db_max_open_connections"`
	DBSSLMode            string `toml:"db_ssl_mode"`

	// Interval at which accumulated messages should be written to Postgres,
	// in milliseconds (default 1000, i.e. 1 second)
	FlushInterval uint32 `toml:"flush_interval"`
	// Number of messages that triggers a write to Postgres (default 10000)
	FlushCount int `toml:"flush_count"`
}

func (po *PostgresOutput) ConfigStruct() interface{} {
	return &PostgresOutputConfig{
		AllowMissingMessageFields: true,
		DBConnectionTimeout:       5,
		DBMaxOpenConnections:      10,
		DBSSLMode:                 "require",
		FlushInterval:             uint32(1000),
		FlushCount:                10000,
		InsertSchema:              "public",
	}
}

func (po *PostgresOutput) Init(rawConf interface{}) error {
	config := rawConf.(*PostgresOutputConfig)
	po.flushInterval = config.FlushInterval
	po.flushCount = config.FlushCount
	po.insertSchema = config.InsertSchema
	po.insertTable = config.InsertTable
	if config.InsertMessageFields == "" {
		return fmt.Errorf("config item 'insert_message_fields' cannot be empty string")
	}
	po.insertMessageFields = strings.Split(config.InsertMessageFields, " ")
	if config.InsertTableColumns == "" {
		return fmt.Errorf("config item 'insert_table_columns' cannot be empty string")
	}
	po.insertTableColumns = strings.Split(config.InsertTableColumns, " ")
	po.allowMissingMessageFields = config.AllowMissingMessageFields
	p := postgres.DBConnectionParams{
		Host:           config.DBHost,
		Port:           config.DBPort,
		DBName:         config.DBName,
		User:           config.DBUser,
		Password:       config.DBPassword,
		ConnectTimeout: config.DBConnectionTimeout,
		SSLMode:        config.DBSSLMode,
	}
	db, err := postgres.New(&p)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(config.DBMaxOpenConnections)
	po.db = db
	return nil
}

func (o *PostgresOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	defer o.db.Close()
	var wg sync.WaitGroup
	wg.Add(1)

	committers := o.makeCommitters(or, 5, &wg)
	go o.receiver(or, committers, &wg)

	wg.Wait()
	return
}

// Runs in a separate goroutine, accepting incoming messages, buffering output
// data until the ticker triggers the buffered data should be put onto the
// committer channel.
func (o *PostgresOutput) receiver(
	or OutputRunner, committers chan<- [][]interface{}, wg *sync.WaitGroup,
) {
	var pack *PipelinePack

	ticker := time.Tick(time.Duration(o.flushInterval) * time.Millisecond)
	batch := [][]interface{}{}

	for ok := true; ok; {
		select {
		case pack, ok = <-or.InChan():
			if !ok {
				// Closed inChan => we're shutting down, flush data
				committers <- batch
				close(committers)
				break
			}
			// Read values from message fields
			vals, err := o.convertMessageToValues(pack.Message, o.insertMessageFields)
			pack.Recycle()
			if err != nil {
				or.LogError(err)
			} else {
				batch = append(batch, vals)
				if len(batch) >= o.flushCount {
					committers <- batch
					batch = [][]interface{}{}
				}
			}
		case <-ticker:
			committers <- batch
			batch = [][]interface{}{}
		}
	}
	wg.Done()
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, bulk inserts it into Postgres, and puts the now empty buffer on the
// return channel for reuse.
func (o *PostgresOutput) makeCommitters(
	or OutputRunner,
	count int,
	wg *sync.WaitGroup,
) chan<- [][]interface{} {
	batches := make(chan [][]interface{})

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			for batch := range batches {
				if len(batch) <= 0 {
					continue
				}

				done := o.commit(or, batch)
				timeout := time.NewTimer(time.Minute)

				select {
				case <-done:
					timeout.Stop()
				case <-timeout.C:
					or.LogError(errors.New("Postgres insert took more than 60s."))
				}
			}
			wg.Done()
		}(i)
	}

	return batches
}

func (o *PostgresOutput) commit(or OutputRunner, batch [][]interface{}) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		err := o.db.Insert(o.insertSchema, o.insertTable, o.insertTableColumns, batch)
		if err != nil {
			or.LogError(err)
		}
		done <- struct{}{}
	}()

	return done
}

// convertMessageToValue reads a Heka Message and returns a slice of field values
func (po *PostgresOutput) convertMessageToValues(m *message.Message, insertFields []string) (fieldValues []interface{}, err error) {
	fieldValues = []interface{}{}
	missingFields := []string{}
	for _, field := range insertFields {
		// Special case: get "Timestamp" from Heka message
		if field == "Timestamp" {
			// Convert Heka time (Unix timestamp in nanoseconds) to Golang time
			v := time.Unix(0, m.GetTimestamp())
			fieldValues = append(fieldValues, v)
		} else {
			v, ok := m.GetFieldValue(field)
			if !ok {
				// If configured to do so, write NULL when a FieldValue isn't found in the Heka message
				if po.allowMissingMessageFields {
					v = nil
				} else {
					missingFields = append(missingFields, field)
					continue
				}
			}
			fieldValues = append(fieldValues, v)
		}
	}

	if len(missingFields) > 0 {
		return []interface{}{}, fmt.Errorf("message is missing expected fields: %s", strings.Join(missingFields, ", "))
	}

	return fieldValues, nil
}

func init() {
	RegisterPlugin("PostgresOutput", func() interface{} {
		return new(PostgresOutput)
	})
}
