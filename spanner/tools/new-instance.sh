#!/bin/sh

set -eux

SPANNER_PROJECT=gotaface
SPANNER_INSTANCE=test
SPANNER_DATABASE=example

gcloud config set project "${SPANNER_PROJECT}"
gcloud config set auth/disable_credentials true
gcloud config set api_endpoint_overrides/spanner http://spanner:9020/
gcloud spanner instances describe "${SPANNER_INSTANCE}" \
    || gcloud spanner instances create "${SPANNER_INSTANCE}" --config=emulator-config --description="Instance for gotaface test"

gcloud spanner databases describe "${SPANNER_DATABASE}" --instance="${SPANNER_INSTANCE}" \
    && gcloud spanner databases delete "${SPANNER_DATABASE}" --instance="${SPANNER_INSTANCE}" \
    || true
gcloud spanner databases create "${SPANNER_DATABASE}" --instance="${SPANNER_INSTANCE}"
