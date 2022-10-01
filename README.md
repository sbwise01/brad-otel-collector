# brad-otel-collector
A custom opentelemetry collector build that includes a subset of components from opentelemetry, opentelemetry contrib, and custom components

## Building
`go build -o brad-otel-collector ./cmd/brad-otel-collector`

## Unit tests
```
go test -v github.com/sbwise01/brad-otel-collector/pkg/receiver/awss3receiver
go test -v github.com/sbwise01/brad-otel-collector/pkg/util
```

## Integration tests
```
docker build -t brad-otel-collector-ci:1.0.0 -f tests/Dockerfile .
docker run -it --rm --name brad-otel-collector-ci --entrypoint /bin/bash brad-otel-collector-ci:1.0.0

# From inside docker container:
  /srv/awss3receiver/bin/run_ci_test.sh
```
