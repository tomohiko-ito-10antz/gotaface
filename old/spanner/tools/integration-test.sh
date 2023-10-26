#!/bin/sh

export GOTAFACE_TEST_SPANNER_PROJECT=gotaface
export GOTAFACE_TEST_SPANNER_INSTANCE=test

mkdir -p "cover/spanner"
go test -cover "./spanner/..." -coverprofile="cover/spanner/cover.out"  && go tool cover -html="cover/spanner/cover.out" -o "cover/spanner/cover.html"
