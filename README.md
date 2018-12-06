# Rumour

[![GoDoc](https://godoc.org/github.com/bsm/rumour?status.svg)](https://godoc.org/github.com/bsm/rumour)
[![Build Status](https://travis-ci.org/bsm/rumour.svg?branch=master)](https://travis-ci.org/bsm/rumour)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/rumour)](https://goreportcard.com/report/github.com/bsm/rumour)

Rumour is a leaner, meaner and more easily configurable version of LinkedIn's [Burrow](https://github.com/linkedin/Burrow). It is a small binary which can perform continuous monitoring tasks of [Apache Kafka](https://kafka.apache.org/) consumer states, offsets and lags.

## Setup

Download the latest release from [GitHub](https://github.com/bsm/rumour/releases) or run it directly via Docker:

```shell
docker run --rm \
  -e RUMOUR_CLUSTERS=default \
  -e RUMOUR_DEFAULT_BROKERS=kafka:9092 \
  blacksquaremedia/rumour:latest
```

## Configuration

All configuration is done via ENV variables. The main configuration parameters are:

* `RUMOUR_CLUSTERS` - a comma-separated list of cluster names to monitor. Default: `default`
* `RUMOUR_HTTP_ADDR` - the address to listen on. Default: `:8080`.

Additonal configuration can be specified for each of the named clusters using the `RUMOUR_{cluster}_` prefix.

* `RUMOUR_{cluster}_BROKERS` - a comma-separated list of broker addresses.
* `RUMOUR_{cluster}_META_REFRESH` - metadata refresh interval. Default: 180s.
* `RUMOUR_{cluster}_OFFSET_REFRESH` - offset refresh interval. Default: 30s.

Example:

```shell
RUMOUR_CLUSTERS=main,prio \
RUMOUR_MAIN_BROKERS=10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092 \
RUMOUR_PRIO_BROKERS=10.0.0.1:9192,10.0.0.2:9192,10.0.0.3:9192 \
RUMOUR_PRIO_META_REFRESH=120s \
./rumour
```

## Integrations

* [datadog](./integrations/datadog/) - a Datadog check to pull metrics out of Rumour and push them to [Datadog](https://www.datadoghq.com/).

## API

Rumour exposes metrics via a HTTP API for data collectors. It is loosely based on [Burrow's](https://github.com/linkedin/Burrow/wiki/HTTP-Endpoint) HTTP endpoints.

### Error Responses

For bad requests, the API will return an appropriate HTTP status code and a JSON body containing:

```json
{
  "error": true,
  "message": "Full error message"
}
```

### Endpoints

#### Health check:

```
GET /healthz
```

#### List clusters:

```
GET /v1/clusters
```

```json
{
  "clusters": ["main", "prio"]
}
```

#### Show cluster details:

```
GET /v1/clusters/NAME
```

```json
{
  "cluster": "main",
  "brokers": ["10.0.0.1:9092", "10.0.0.2:9092", "10.0.0.3:9092"],
  "topics": ["my-topic"],
  "consumers": ["consumer-x", "consumer-y"]
}
```

#### Show cluster topics:

```
GET /v1/clusters/NAME/topics
```

```json
{
  "cluster": "main",
  "topics": ["my-topic"]
}
```

#### Show cluster consumers:

```
GET /v1/clusters/NAME/consumers
```

```json
{
  "cluster": "main",
  "consumers": ["consumer-x", "consumer-y"]
}
```

#### Show topic:

```
GET /v1/clusters/NAME/topics/TOPIC
```

```json
{
  "cluster": "main",
  "topic": "my-topic",
  "offsets": [1041, 1042, 1043, 1044]
}
```

#### Show consumer:

```
GET /v1/clusters/NAME/consumers/GROUP
```

```json
{
  "cluster": "main",
  "consumer": "consumer-x",
  "topics": [
    {
      "topic": "my-topic",
      "timestamp": 1515151515,
      "offsets": [
        {"offset":1037, "lag": 4},
        {"offset":1041, "lag": 1},
        {"offset":1029, "lag": 14},
        {"offset":1044, "lag": 0}
      ]
    }
  ]
}
```
