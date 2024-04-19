package logging

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap/zapcore"
)

type KafkaRecord kgo.Record

func (r *KafkaRecord) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("topic", r.Topic)
	enc.AddInt32("partition", r.Partition)
	enc.AddInt64("offset", r.Offset)
	return nil
}
