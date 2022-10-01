################################
#	Building Stage         #
#			       #
################################
FROM golang:1.17.10-buster as build

# start creating the directory and copy in all the files
ARG AGENT_DIR=/srv/otel-collector
WORKDIR ${AGENT_DIR}

# Separate these lines out so they are cached in different layers
# As a result, go mod download will only re-run if go.sum/go.mod has changed.
# Additionally, in order to do this without breaking go download functionality,
# include any sources referenced in go.mod replace directives that are local to
# the module
COPY go.sum .
COPY go.mod .
COPY generated ./generated

# Copy the source code and build the binary
COPY . .
RUN go mod tidy
RUN go build -o brad-otel-collector ./cmd/brad-otel-collector

################################
#	Final Stage            #
#			       #
################################
FROM golang:1.17.10-buster

ARG AGENT_DIR=/srv/otel-collector
ARG TEST_DIR=/srv/awss3receiver
RUN mkdir ${TEST_DIR}
COPY tests/files ${TEST_DIR}
COPY --from=build ${AGENT_DIR}/brad-otel-collector ${TEST_DIR}/bin/otel-collector
ADD https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2022-05-26T05-48-41Z ${TEST_DIR}/bin/minio
RUN chmod 755 ${TEST_DIR}/bin/*

WORKDIR ${TEST_DIR}
