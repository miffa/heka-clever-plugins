#!/bin/sh
# Lua tests
sudo apt-get install lua -y

# Go tests
createdb -h localhost -U postgres drone
psql -U postgres -h localhost -c "CREATE TABLE \"mock_table\" (s text, i int)" drone
cd postgres
go get ./...
go get github.com/stretchr/testify/assert
go test
