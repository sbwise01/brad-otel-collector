##########################
# Build stage
##########################
FROM golang:1.17.10-buster as build

ARG SERVICE_DIR=/srv/otel-collector
RUN mkdir ${SERVICE_DIR}
WORKDIR ${SERVICE_DIR}

COPY . .
RUN go mod tidy
RUN go build -o brad-otel-collector ./cmd/brad-otel-collector

##########################
# Final stage
##########################
FROM golang:1.17.10-buster

ARG SERVICE_DIR=/srv/otel-collector
RUN mkdir ${SERVICE_DIR}
COPY --from=build ${SERVICE_DIR}/brad-otel-collector ${SERVICE_DIR}/brad-otel-collector

WORKDIR ${SERVICE_DIR}

