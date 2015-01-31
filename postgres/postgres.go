package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
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
func buildInsertQuery(table string, values [][]interface{}) (string, error) {
	// Validate input
	if table == "" {
		return "", fmt.Errorf("table name cannot be empty string")
	}
	if len(values) <= 0 {
		return "", fmt.Errorf("requires at least one value")
	}

	// Build query
	q := fmt.Sprintf("INSERT INTO \"%s\" VALUES ", table)
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
func (pi *PostgresDB) Insert(table string, values [][]interface{}) error {
	q, err := buildInsertQuery(table, values)
	if err != nil {
		return err
	}
	flatValues := flatten(values)
	_, err = pi.DB.Query(q, flatValues...)
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
