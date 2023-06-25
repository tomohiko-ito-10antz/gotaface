#!/bin/sh

export ENV_SQLITE_TEST_DIR=$(pwd)/sqlite3/test_db

mkdir -p "${ENV_SQLITE_TEST_DIR}"
go test ./sqlite3/cli/dbschema/...
go test ./sqlite3/cli/dbdelete/...
