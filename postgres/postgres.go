package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strings"
)

type PostgresDB struct {
	*sql.DB
}

type DBConnectionParams struct {
	Host           string
	Port           int
	DBName         string
	User           string
	Password       string
	SSLMode        string
	ConnectTimeout int
}

func New(p *DBConnectionParams) (*PostgresDB, error) {
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d sslmode=%s", p.Host, p.Port, p.DBName, p.ConnectTimeout, p.SSLMode)
	log.Println("Connecting to Postgres:", source)
	source += fmt.Sprintf(" user=%s password=%s", p.User, p.Password)
	db, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	return &PostgresDB{DB: db}, nil
}

// buildInsertQuery returns string of prepared query for inserting one or more values
func buildInsertQuery(table string, columns []string, values [][]interface{}) (string, error) {
	// Validate input
	if table == "" {
		return "", fmt.Errorf("table name cannot be empty string")
	}
	if len(values) <= 0 {
		return "", fmt.Errorf("requires at least one value")
	}

	// Build query
	q := fmt.Sprintf("INSERT INTO \"%s\" ", table)
	// If columns are specified, add them to query
	columnCount := len(columns)
	if columnCount > 0 {
		q += "("
		q += strings.Join(columns, ", ")
		q += ") "
	}
	q += "VALUES "
	fieldCount := -1
	for valIdx, val := range values {
		// Validate this value
		if fieldCount != -1 && len(val) != fieldCount {
			return "", fmt.Errorf("all values must have the same number of fields. first value had %d fields", fieldCount)
		}
		fieldCount = len(val)
		if fieldCount <= 0 {
			return "", fmt.Errorf("value must have at least one field")
		}
		if columnCount != 0 && fieldCount != columnCount {
			// If inserting into specific columns, verify that we have the right number of elements in each value
			return "", fmt.Errorf("value has %d elements, so cannot insert into %d columns", fieldCount, columnCount)
		}

		// Add value to the query
		if valIdx > 0 {
			q += ", "
		}
		q += "("
		for fieldIdx, _ := range val {
			if fieldIdx > 0 {
				q += ", "
			}
			q += "$"
			q += fmt.Sprintf("%d", valIdx*fieldCount+fieldIdx+1)
		}
		q += ")"
	}
	return q, nil
}

// Insert one or more values into DB
func (pi *PostgresDB) Insert(table string, columns []string, values [][]interface{}) error {
	q, err := buildInsertQuery(table, columns, values)
	if err != nil {
		return err
	}
	flatValues := flatten(values)
	rows, err := pi.DB.Query(q, flatValues...)
	if rows != nil {
		// Close the connection, to avoid "pq: sorry, too many clients already" error
		defer rows.Close()
	}
	if err != nil {
		return err
	}
	return nil
}

func flatten(input [][]interface{}) []interface{} {
	f := []interface{}{}
	for _, i := range input {
		f = append(f, i...)
	}
	return f
}
