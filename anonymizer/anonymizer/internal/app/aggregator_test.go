package app_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/app"
)

type AggregatorSuite struct {
	suite.Suite
	agg *app.Aggregator
}

func (s *AggregatorSuite) SetupTest() {
	s.agg = app.NewAggregator("http_log")
}

func (s *AggregatorSuite) Test_SingleRecord() {
	record := app.LogRecord{
		KafkaPartition:      1,
		KafkaOffset:         2,
		TimestampEpochMilli: 3,
		ResourceID:          4,
		BytesSent:           5,
		RequestTimeMilli:    6,
		ResponseStatus:      7,
		CacheStatus:         "HIT",
		Method:              "GET",
		RemoteAddr:          "1.2.3.4",
		URL:                 "/api/v1/resources/42",
	}

	s.NoError(s.agg.AppendRecord(&record))
	s.Equal(
		`INSERT INTO http_log VALUES ('1:2', 3, 4, 5, 6, 7, 'HIT', 'GET', '1.2.3.4', '/api/v1/resources/42')`,
		s.agg.Aggregate().String(),
	)
}

func TestAggregatorSuite(t *testing.T) {
	suite.Run(t, new(AggregatorSuite))
}
