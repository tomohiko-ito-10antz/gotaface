#!/bin/sh

export ENV_SQLITE_TEST_DIR=$(pwd)/sqlite/test_db

mkdir -p "${ENV_SQLITE_TEST_DIR}"
go test ./sqlite/cli/dbschema/...
