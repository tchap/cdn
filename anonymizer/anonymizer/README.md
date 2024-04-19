# Anonymizer

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