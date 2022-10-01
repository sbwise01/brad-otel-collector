#!/usr/bin/env bash

export TEST_DIR=/srv/awss3receiver
export MINIO_ROOT_USER=awss3receiver
export MINIO_ROOT_PASSWORD=supersecretpassword

${TEST_DIR}/bin/minio server ${TEST_DIR}/data --address ":9000" --console-address ":9001"
