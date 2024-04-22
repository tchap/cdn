package app

import (
	"capnproto.org/go/capnp/v3"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/logging"
	"github.com/tchap/cdn/anonymizer/anonymizer/internal/messages"
	"github.com/tchap/cdn/anonymizer/anonymizer/internal/pipeline"
)

type LogRecord struct {
	KafkaPartition int32
	KafkaOffset    int64

	TimestampEpochMilli uint64
	ResourceID          uint64
	BytesSent           uint64
	RequestTimeMilli    uint64
	ResponseStatus      uint16
	CacheStatus         string
	Method              string
	RemoteAddr          string
	URL                 string
}

type MessageDecoder struct {
	logger *zap.Logger
}

func NewMessageDecoder(logger *zap.Logger) *MessageDecoder {
	return &MessageDecoder{logger: logger}
}

func (p MessageDecoder) DecodeMessage(r *kgo.Record) (*LogRecord, error) {
	// Unmarshal.
	msg, err := capnp.Unmarshal(r.Value)
	if err != nil {
		p.logger.Warn(
			"Failed to unmarshal capnp message.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	// Decode.
	rec, err := messages.ReadRootHttpLogRecord(msg)
	if err != nil {
		p.logger.Warn(
			"Failed to decode HTTP log record.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	remoteAddr, err := rec.RemoteAddr()
	if err != nil {
		p.logger.Warn(
			"Failed to decode log record remote address.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	cacheStatus, err := rec.CacheStatus()
	if err != nil {
		p.logger.Warn(
			"Failed to decode log record cache status.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	method, err := rec.Method()
	if err != nil {
		p.logger.Warn(
			"Failed to decode log record method.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	u, err := rec.Url()
	if err != nil {
		p.logger.Warn(
			"Failed to decode log record URL.",
			zap.Object("message", (*logging.KafkaRecord)(r)),
			zap.Error(err),
		)
		return nil, pipeline.ErrSkipRecord
	}

	return &LogRecord{
		KafkaPartition:      r.Partition,
		KafkaOffset:         r.Offset,
		TimestampEpochMilli: rec.TimestampEpochMilli(),
		ResourceID:          rec.ResourceId(),
		BytesSent:           rec.BytesSent(),
		RequestTimeMilli:    rec.RequestTimeMilli(),
		ResponseStatus:      rec.ResponseStatus(),
		CacheStatus:         cacheStatus,
		Method:              method,
		RemoteAddr:          remoteAddr,
		URL:                 u,
	}, nil
}
