package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	KafkaBrokers       []string `envconfig:"KAFKA_BROKERS"        required:"true"`
	KafkaTopic         string   `envconfig:"KAFKA_TOPIC"          required:"true"`
	KafkaClientID      string   `envconfig:"KAFKA_ClIENT_ID"      required:"true"`
	KafkaConsumerGroup string   `envconfig:"KAFKA_CONSUMER_GROUP" required:"true"`
	KafkaFetchMaxMB    int32    `envconfig:"KAFKA_FETCH_MAX_MB"   required:"true"`

	ClickHouseURL            string        `envconfig:"CLICKHOUSE_URL"              required:"true"`
	ClickHouseTableName      string        `envconfig:"CLICKHOUSE_TABLE_NAME"       required:"true"`
	ClickHousePushPeriod     time.Duration `envconfig:"CLICKHOUSE_PUSH_PERIOD"      required:"true"`
	ClickHousePushRetryCount int           `envconfig:"CLICKHOUSE_PUSH_RETRY_COUNT" required:"true"`

	ShutdownTimeout time.Duration `envconfig:"SHUTDOWN_TIMEOUT" default:"5s"`

	LogLevel      zapcore.Level `envconfig:"LOG_LEVEL" default:"info"`
	KafkaLogLevel zapcore.Level `envconfig:"KAFKA_LOG_LEVEL" default:"error"`
}

func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, errors.Wrap(err, "failed to load config")
	}
	return &c, nil
}
