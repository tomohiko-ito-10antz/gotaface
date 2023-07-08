#!/bin/sh

export ENV_SQLITE_TEST_DIR=$(pwd)/sqlite3/test_db

mkdir -p "${ENV_SQLITE_TEST_DIR}"

mkdir -p "cover/sqlite3"
go test -cover "./sqlite3/..." -coverprofile="cover/sqlite3/cover.out"  && go tool cover -html="cover/sqlite3/cover.out" -o "cover/sqlite3/cover.html"
