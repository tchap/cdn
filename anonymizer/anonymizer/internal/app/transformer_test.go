package app_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/app"
)

type LogRecordAnonymizerSuite struct {
	suite.Suite
	transformer app.LogRecordAnonymizer
}

func (s *LogRecordAnonymizerSuite) TestRemoteAddrAnonymized() {
	// Make sure only RemoteAddr is modified as expected.
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
	// TransformRecord modified the record in place, so let's make a copy.
	src := record
	actual := s.transformer.TransformRecord(&src)
	expected := record
	expected.RemoteAddr = "1.2.3.X"
	s.Equal(&expected, actual)
}

func TestLogRecordAnonymizerSuite(t *testing.T) {
	suite.Run(t, new(LogRecordAnonymizerSuite))
}
