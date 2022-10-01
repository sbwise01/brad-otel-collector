#!/usr/bin/env bash

export TEST_DIR=/srv/awss3receiver
export AWS_ACCESS_KEY_ID=awss3receiver
export AWS_SECRET_ACCESS_KEY=supersecretpassword

${TEST_DIR}/bin/otel-collector --config=${TEST_DIR}/etc/otel-collector-config.yml
