# awss3receiver
An opentelemetry receiver that processes CloudWatch Metric Streams data in OTLP format from an AWS S3 bucket.

## Example configuration
```
...
receivers:
  awss3:
    bucket_name: metricstreams-quickfull-k2wzmu-vcp0hesq
    bucket_region: us-east-1
    bucket_io_timeout: 30s
    prefix: path/to/metrics
    gzip_compression: true
    checkpoint_key: awss3receiver_checkpoint.txt
    collection_interval: 1m
    scan_offset: 5m
    processing_delay: 2m
    filter_stale_data_points: true
    staleness_threshold: 15m
    log_level: DEBUG
    extra_labels:
      team: telescope
      application: test
...
```
- **bucket_name**: The name of the AWS S3 bucket that contains CloudWatch Metric Streams output in OTLP format
- **bucket_region**: The AWS region the bucket is located in
- **bucket_io_timeout**: The time duration for S3 download and upload IO operations before it times out.  Defaults to 30s (30 seconds).
- **bucket_endpoint**: An optional hostname:port used to alter the S3 server endpoint used by the receiver, mainly used for CI testing using minio server to mock S3. Example value: localhost:9000
- **prefix**:  If you used a prefix location when setting up the CloudWatch Metric Stream, provide that value here.  Otherwise leave out for default prefix.
- **gzip_compression**:  If you setup your Firehose delivery stream to write objects compressed in gzip format, set this to true.  Otherwise set to false.
- **checkpoint_key**: A bucket key the receiver uses to write checkpoint data to (i.e. the last bucket item processed) so that it does not re-process the same data. Default to awss3receiver_checkpoint.txt
- **collection_interval**: How frequently the receiver runs to scan for new bucket objects
- **scan_offset**: The amount of time previous to current time of collection to scan for new bucket objects.  Defaults to 5m (5 minutes).
- **processing_delay**: The amount of time previous to current time that the receiver will not process bucket objects.  Needed because s3 objects can land out out of order, e.g. `$stream-name-2022-03-16-00-03-xxx` can land before `$stream-name-2022-03-16-00-02-xxx`. We must process objects ordered by the time indicated the object name, because all metric datapoints inside an s3 object fall in the same time range indicated by the name. We must send datapoints to Prometheus in timestamp order to avoid out of order sample errors. Processing delay ensures that we wait long enough. We only process objects written before (now-processing_delay), assuming that at the time of processing, no more objects will land in the bucket that were created before (now-processing_delay).  Defaults to 2m (2 minutes).
- **filter_stale_data_points**: Set this to true to have the receiver filter out data points with timestamps past the `staleness_threshold`.
- **staleness_threshold**: The amount of time previous to current time to consider metric datapoints to be stale based on their metric timestamp.  These metric datapoints will be eliminated and not passed to the processing pipeline.  Defaults to 15m (15 minutes).
- **log_level**: The level of logging to produce
- **extra_labels**: Additional labels to add to each processed metric


## Local testing procedure
1. Provision a CloudWatch metric stream in an AWS account using S3 option and OTLP data format and note the S3 bucket name
1. Provision an IAM user and access keys and give user List*, Get*, and PutObject permissions to S3 bucket
1. Use example configuration below, but update for S3 bucket name and region and update/remove extra_labels and save to local file.  This configuration for otel-collector will run the awss3receiver and output its metrics to a file path on the running Docker container
  ```
  ---
  receivers:
    awss3:
      bucket_name: metricstreams-quickfull-k2wzmu-vcp0hesq
      bucket_region: us-east-1
      gzip_compression: false
      checkpoint_key: awss3receiver_checkpoint.txt
      collection_interval: 1m
      scan_offset: 5m
      processing_delay: 2m
      filter_stale_data_points: true
      staleness_threshold: 5m
      log_level: DEBUG
      extra_labels:
        owner: brad
        environ: test:all

  exporters:
    file:
      path: /tmp/otel-exported-metrics.json

  extensions:
    memory_ballast:
      size_mib: 200
    health_check:
      port: 10696
    pprof:
      endpoint: localhost:10697
    zpages:
      endpoint: localhost:10698

  service:
    extensions:
    - memory_ballast
    - health_check
    - pprof
    - zpages
    pipelines:
      metrics/k8s-host:
        receivers:
        - awss3
        exporters:
        - file
  ```
1. Build local docker image to use to run test with:  `docker build -t s3-otel-receiver:1.0.84 -f _infra/kube/images/Dockerfile .`
1. Set `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID` environment variables using values from AWS access keys provisioned in step 2
1. Run the docker image in interactive mode, injecting the AWS access keys from your environment variables and the otel-collector configuration file created in step 3 `docker run -it --rm --name otelcol --entrypoint /bin/bash -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" -v /path/to/otel-collector-config.yml:/srv/otel-collector/config.yml s3-otel-receiver:1.0.84`
1. Within interactive docker container, run the otel-collector `/srv/otel-collector/otel-collector --config=/srv/otel-collector/config.yml`
1. From another terminal window, exec into docker container `docker exec -it otelcol /bin/bash`
1. View contents of metrics output `cat /tmp/otel-exported-metrics.json`
