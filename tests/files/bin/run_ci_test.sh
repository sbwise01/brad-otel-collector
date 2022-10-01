#!/usr/bin/env bash

export TEST_DIR=/srv/awss3receiver

echo "Preparing test inputs"
TEST_FILES=`ls /srv/awss3receiver/data/testbucket/ | sort`
for TEST_FILE in ${TEST_FILES}
do
  ${TEST_DIR}/bin/s3_mv.sh ${TEST_FILE}
  sleep 2
done
# expected output file is compressed due to repo limit on file size
gunzip "${TEST_DIR}/data/expected/otel-exported-metrics.json.gz"
mkdir -p ${TEST_DIR}/data/exported

echo "Starting up mock S3 server"
nohup ${TEST_DIR}/bin/start_minio.sh &

echo "Starting up otel-collector"
nohup ${TEST_DIR}/bin/start_otel_collector.sh &

echo "Waiting for otel-collector to export metrics"
sleep 30

echo "Verifying exported output"
diff --brief "${TEST_DIR}/data/expected/otel-exported-metrics.json" "${TEST_DIR}/data/exported/otel-exported-metrics.json"
COMPARE_STATUS=$?
if [ ${COMPARE_STATUS} -ne 0 ]
then
  echo "Exported output does not equal expected output"
  exit ${COMPARE_STATUS}
else
  echo "CI tests passed"
fi
