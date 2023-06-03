#!/bin/sh

export GOTAFACE_TEST_SQLITE_SCHEMA_DB_DIR=./
go test ./sqlite/schema/...