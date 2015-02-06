package heka_clever_plugins

import (
	"fmt"
	"github.com/Clever/heka-clever-plugins/postgres"
	_ "github.com/lib/pq"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"strings"
	"sync"
	"time"
)

type PostgresOutput struct {
	db                        *postgres.PostgresDB
	insertTable               string
	insertMessageFields       []string
	insertTableColumns        []string
	batchChan                 chan [][]interface{}
	backChan                  chan [][]interface{}
	flushInterval             uint32
	flushCount                int // Max messages before flush
	allowMissingMessageFields bool
}

type PostgresOutputConfig struct {
	// Table name and colums. Message fields to write.
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
	}
}

func (po *PostgresOutput) Init(rawConf interface{}) error {
	config := rawConf.(*PostgresOutputConfig)
	po.flushInterval = config.FlushInterval
	po.flushCount = config.FlushCount
	po.batchChan = make(chan [][]interface{})
	po.backChan = make(chan [][]interface{}, 2)
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
	wg.Add(2)
	go o.receiver(or, &wg)
	go o.committer(or, &wg)
	wg.Wait()
	return
}

// Runs in a separate goroutine, accepting incoming messages, buffering output
// data until the ticker triggers the buffered data should be put onto the
// committer channel.
func (o *PostgresOutput) receiver(or OutputRunner, wg *sync.WaitGroup) {
	var (
		pack  *PipelinePack
		count int
	)
	ok := true
	ticker := time.Tick(time.Duration(o.flushInterval) * time.Millisecond)
	outBatch := [][]interface{}{}
	inChan := or.InChan()

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				// Closed inChan => we're shutting down, flush data
				if len(outBatch) > 0 {
					o.batchChan <- outBatch
				}
				close(o.batchChan)
				break
			}
			// Read values from message fields
			val, e := o.convertMessageToValues(pack.Message, o.insertMessageFields)
			pack.Recycle()
			if e != nil {
				or.LogError(e)
			} else {
				outBatch = append(outBatch, val)
				if count = count + 1; o.CheckFlush(count, len(outBatch)) {
					if len(outBatch) > 0 {
						// This will block until the other side is ready to accept
						// this batch, so we can't get too far ahead.
						o.batchChan <- outBatch
						outBatch = <-o.backChan
						count = 0
					}
				}
			}
		case <-ticker:
			if len(outBatch) > 0 {
				// This will block until the other side is ready to accept
				// this batch, freeing us to start on the next one.
				o.batchChan <- outBatch
				outBatch = <-o.backChan
				count = 0
			}
		}
	}
	wg.Done()
}

// Runs in a separate goroutine, waits for buffered data on the committer
// channel, bulk inserts it into Postgres, and puts the now empty buffer on the
// return channel for reuse.
func (o *PostgresOutput) committer(or OutputRunner, wg *sync.WaitGroup) {
	initBatch := [][]interface{}{}
	o.backChan <- initBatch
	var outBatch [][]interface{}

	for outBatch = range o.batchChan {
		if err := o.db.Insert(o.insertTable, o.insertTableColumns, outBatch); err != nil {
			or.LogError(err)
		}
		outBatch = outBatch[:0]
		o.backChan <- outBatch
	}
	wg.Done()
}

func (o *PostgresOutput) CheckFlush(count int, length int) bool {
	if count >= o.flushCount {
		return true
	}
	return false
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
		return []interface{}{}, fmt.Errorf("message is missing expected fields:", missingFields)
	}

	return fieldValues, nil
}

func init() {
	RegisterPlugin("PostgresOutput", func() interface{} {
		return new(PostgresOutput)
	})
}
