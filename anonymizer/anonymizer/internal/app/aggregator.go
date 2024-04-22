package app

import (
	"bytes"
	"fmt"
	"strconv"
)

type Aggregator struct {
	query    bytes.Buffer
	rowCount int
}

func NewAggregator(tableName string) *Aggregator {
	agg := &Aggregator{}
	fmt.Fprintf(&agg.query, "INSERT INTO %s VALUES ", tableName)
	return agg
}

func (agg *Aggregator) AppendRecord(r *LogRecord) error {
	if agg.rowCount != 0 {
		agg.query.WriteString(",")
	}
	fmt.Fprintf(
		&agg.query,
		`('%d:%s', %d, %d, %d, %d, %d, '%s', '%s', '%s', '%s')`,
		r.KafkaPartition, strconv.FormatInt(r.KafkaOffset, 10),
		r.TimestampEpochMilli,
		r.ResourceID,
		r.BytesSent,
		r.RequestTimeMilli,
		r.ResponseStatus,
		r.CacheStatus,
		r.Method,
		r.RemoteAddr,
		r.URL,
	)
	agg.rowCount++
	return nil
}

func (agg *Aggregator) Aggregate() *bytes.Buffer {
	return &agg.query
}
