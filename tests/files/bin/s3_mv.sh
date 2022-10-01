#!/usr/bin/env bash
export TEST_DIR=/srv/awss3receiver

# Prepare S3 test data
export CURRENT_YEAR=`date +%Y`
export CURRENT_MONTH=`date +%m`
export CURRENT_DAY=`date +%d`
export CURRENT_HOUR=`date +%H`
export CURRENT_MINUTE=`date +%M`
export CURRENT_SECOND=`date +%S`
mkdir -p "${TEST_DIR}/data/testbucket/${CURRENT_YEAR}/${CURRENT_MONTH}/${CURRENT_DAY}/${CURRENT_HOUR}"
mv "${TEST_DIR}/data/testbucket/${1}" "${TEST_DIR}/data/testbucket/${CURRENT_YEAR}/${CURRENT_MONTH}/${CURRENT_DAY}/${CURRENT_HOUR}/datadog-metrics-stream-3-${CURRENT_YEAR}-${CURRENT_MONTH}-${CURRENT_DAY}-${CURRENT_HOUR}-${CURRENT_MINUTE}-${CURRENT_SECOND}-${1}"
