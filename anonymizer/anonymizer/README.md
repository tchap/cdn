# Anonymizer

See the parent directory for what this service is about.

## Setup

ClickHouse tables need to be created before the service is started.
This currently happens manually. Start by connecting to ClickHouse:

```
$ docker-compose run clickhouse-client
```

Then execute the following queries:

```
CREATE TABLE http_log (
   uuid               String,
   timestamp          DateTime,
   resource_id        UInt64,
   bytes_sent         UInt64,
   request_time_milli UInt64,
   response_status    UInt16,
   cache_status       LowCardinality(String),
   method             LowCardinality(String),
   remote_addr        String,
   url                String
)
ENGINE = ReplacingMergeTree
ORDER BY uuid;

CREATE TABLE http_log_agg (
   cache_status       LowCardinality(String),
   response_status    UInt16,
   resource_id        UInt64,
   remote_addr        String,

   bytes_sent_total         SimpleAggregateFunction(sum, UInt64),
   request_time_milli_total SimpleAggregateFunction(sum, UInt64),
   request_count_total      AggregateFunction(count)
)
ENGINE = AggregatingMergeTree
ORDER BY (cache_status, response_status, resource_id, remote_addr)
SETTINGS index_granularity = 1024, index_granularity_bytes = 0;

CREATE MATERIALIZED VIEW http_log_agg_mv
TO http_log_agg
AS
   SELECT
      cache_status,
      response_status,
      resource_id,
      remote_addr,
      sum(bytes_sent) AS bytes_sent_total,
      sum(request_time_milli) AS request_time_milli_total,
      countState() AS request_count_total
   FROM http_log
   GROUP BY cache_status, response_status, resource_id, remote_addr;
```

The next possible step is to start `anonymizer` to be able to watch the logs.
This will create the Kafka topic as well.

Note that log verbosity can be controlled using `LOG_LEVEL` and `KAFKA_LOG_LEVEL`
environment variables, which are set to sensible defaults. Setting `KAFKA_LOG_LEVEL`
to `info` makes the service print various Kafka client events.

```
$ docker-compose run anonymizer
```

To start producing Kafka messages, start `http-log-kafka-producer`.

## Design

There are 3 components that constitute the service:

1. **The Kafka consumer loop**. Receives Kafka messages and forwards them to the aggregation loop.
2. **The aggregation loop**. Receives Kafka messages, does decoding, transformation, aggregation,
   then forwards the aggregated state to the pusher loop when the aggregation time window is over.
3. **The pusher loop**. The last thread in the chain pushes aggregated state out.

This architecture means that state aggregation (ClickHouse query generation) is independent
from sending the HTTP request to the ClickHouse API so that it can be happening concurrently.

The pipeline is implemented in a generic way in `internal/pipeline`. Business logic is just filled in.
The pipeline instance for our purposes is implemented in `internal/app`. There we use a `MessageDecoder`
that decodes messages into log records, `RecordTransformer` that does anonymization, `AggregationWindow`
that generates a ClickHouse query from incoming messages, and finally a `Pusher` that sends the query
as an HTTP POST request.

Push query size can be limited by setting `CLICKHOUSE_QUERY_MAX_SIZE_MB` (default `1`). When the query
exceeds the size, the aggregation loop stops processing Kafka messages from the input channel,
leading to the Kafka consumer stopping processing Kafka messages temporarily (the channel is not buffered).

A similar pushback works for the pusher loop. Until the pending request is processed,
the thread will not receive any query to be pushed, eventually blocking the pipeline
all the way to the Kafka message consumer.

Note that the ClickHouse query buffer never shrinks to free memory.
The decoder returns a struct that is newly allocated for each message.
We could use the CapNProto stub `intenal/message.HttpLogRecord` in case we wanted to prevent this,
but it also makes the code a little bit more ugly.

### Kafka Commit Offset Management

The service is made to be at-least-once. This means that Kafka offsets are only committed
once the push request succeeds. This means that some Kafka messages can be processed multiple times.

Generally any crash will cause service termination without any offset commit,
because we need to only commit pushed state. Consumer group rebalancing is not handled in any way,
which means that you can commit an offset from a service instance
while another instance has already processed further with the partition offset.
This seems to be hard to solve when we prioritize output rate limiting, which
means that we cannot push data to flush any time we want.

### ClickHouse

To deduplicate records, a combination of Kafka topic partition and offset is being used
to fill the `uuid` column that the target table is being ordered by.
ReplacingMergeTree engine that is being used is suitable for clearing out duplicate data
in the background in order to save space, but it does not guarantee the absence of duplicates.

XXX: Describe the database scheme.

## Production Grade TODO

- [ ] Messages that were impossible to decode should be forwarded to a given topic.
- [ ] Log records should be validated and invalid records also forwarded to a given topic.
- [ ] Add OTEL metrics. Some basic metrics are currently being printed to logs.
- [ ] Kafka offset commiting logic accesses a map for each message. Can be surely improved to use an array.
- [ ] The service simply crashes when it fails to push N times. Infinite retry is also an option...
- [ ] Handle rate limit exceeded more explicitly, not just using generic retry then crash.
      The best would be to stop consuming aggregate state for a while when rate limit exceeded.
- [ ] Actually we can merge the consumer and aggregator loops into one and save a channel send.
- [ ] Tests!

## Development

CapNProto stubs are committed in `internal/messages`. You can run `make gen` to regenerate.
You will need to install some dependencies for that, though:

1. Temporarily clone https://github.com/capnproto/go-capnp into this directory.
   This is required to be able to include dependencies when running `capnp`.
2. Check `docs/Writing-Schemas-and-Generating-Code.md` to see how to proceed further.
   You will need to install [campnp](https://capnproto.org/install.html) and also
   `go-capnp`, which can be done from the cloned `capnproto/go-capnp`.
