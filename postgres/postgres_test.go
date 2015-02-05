package postgres

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getTestDBConnectionParams() DBConnectionParams {
	return DBConnectionParams{
		Host:           "localhost",
		Port:           5432,
		DBName:         "drone",
		User:           "postgres",
		Password:       "",
		ConnectTimeout: 5,
		SSLMode:        "disable",
	}
}

func Test_connectToDB(t *testing.T) {
	p := getTestDBConnectionParams()
	_, err := New(&p)
	assert.NoError(t, err)
}

func Test_buildInsertQuery(t *testing.T) {
	expected := "INSERT INTO \"mock_table\" VALUES ($1, $2, $3)"
	actual, err := buildInsertQuery("mock_table", [][]interface{}{
		{1, 2, 3},
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_buildMultiInsertQuery(t *testing.T) {
	expected := "INSERT INTO \"mock_table\" VALUES ($1, $2, $3), ($4, $5, $6)"
	actual, err := buildInsertQuery("mock_table", [][]interface{}{
		{1, 2, 3},
		{4, 5, 6},
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_buildInsertQueryErrorsIfNoTable(t *testing.T) {
	_, err := buildInsertQuery("", [][]interface{}{
		{1},
	})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "table name cannot be empty string")
}

func Test_buildInsertQueryErrorsIfNoFields(t *testing.T) {
	_, err := buildInsertQuery("mock_table", [][]interface{}{
		{}, // 1 value, 0 fields
	})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "value must have at least one field")
}
func Test_buildInsertQueryErrorsIfDifferentNumberOfFields(t *testing.T) {
	_, err := buildInsertQuery("mock_table", [][]interface{}{
		{1, 2}, // 2 fields, different number of fields
		{3},
	})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "all values must have the same number of fields. first value had 2 fields")
}

func Test_buildInsertQueryErrorsIfNoValues(t *testing.T) {
	_, err := buildInsertQuery("mock_table", [][]interface{}{})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "requires at least one value")
}

func Test_connectAndInsert(t *testing.T) {
	p := getTestDBConnectionParams()
	postgresInserter, err := New(&p)
	assert.NoError(t, err)
	err = postgresInserter.Insert("mock_table", [][]interface{}{
		{"foo", 1},
	})
	assert.NoError(t, err)
}

func Test_connectAndBulkInsert(t *testing.T) {
	p := getTestDBConnectionParams()
	postgresInserter, err := New(&p)
	assert.NoError(t, err)
	err = postgresInserter.Insert("mock_table", [][]interface{}{
		{"bar", 2},
		{"baz", 3},
	})
	assert.NoError(t, err)
}
