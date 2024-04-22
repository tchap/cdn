# Anonymizer

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

## Limitations

Duplicate data processing can happen, the whole thing is made to be at-least-once.
Any crash will cause duplicate processing. Consumer group rebalancing is not handled
in any way, which means that you can commit an offset from a service instance
while another instance has already processed further with the partition offset.
This seems to be hard to solve when we prioritize output rate limiting, which
means that we cannot push data to flush any time we want.

### ClickHouse

ReplacingMergeTree is suitable for clearing out duplicate data in the background in order to save space,
but it does not guarantee the absence of duplicates.

## TODO

* `internal/app` contains zero abstractions, just a blob of code.
   This could be made completely abstract using generics.
* Messages that were impossible to decode should be forwarded to a given topic.
* Log records should be validated.
* ClickHouse output buffer should totally be limited in size.
* Add OTEL metrics. Some are currently being printed to logs.
* Kafka offset commiting accesses a map for each message. Not cool.
* Handle rate limit exceeded more explicitly, not just using generic retry, then crash.
* To deduplicate records, a combination of Kafka topic partition and offset is being used.
* The service crashes when it fails to push N times. Infinite retry is also an option...