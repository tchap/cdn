package app

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/config"
	"github.com/tchap/cdn/anonymizer/anonymizer/internal/pipeline"
)

type App struct {
	kafka *kgo.Client
	pipe  *pipeline.Pipeline[*LogRecord, string]
}

func New(
	c config.Config,
	logger *zap.Logger,
) (*App, error) {
	// Init the Kafka client.
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(c.KafkaBrokers...),
		kgo.ClientID(c.KafkaClientID),
		kgo.ConsumerGroup(c.KafkaConsumerGroup),
		kgo.ConsumeTopics(c.KafkaTopic),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(1024*1024*c.KafkaFetchMaxMB),
		// This is just for playing around.
		// Normally you do not want to start at the end.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.WithLogger(kzap.New(logger.Named("kafka").WithOptions(zap.IncreaseLevel(c.KafkaLogLevel)))),
	)
	if err != nil {
		logger.Error("Failed to init Kafka client.", zap.Error(err))
		return nil, err
	}

	pipe := pipeline.New(
		logger.Named("pipeline"),
		kafkaClient,
		c.KafkaTopic,
		NewMessageDecoder(logger.Named("parser")),
		LogRecordAnonymizer{},
		NewAggregator(c.ClickHouseTableName),
		c.ClickHousePushPeriod,
		c.ClickHouseQueryMaxSizeMB*1024*1024,
		NewHTTPPostPusher(
			logger.Named("http_post_pusher"), c.ClickHouseURL, c.ClickHousePushRetryCount, c.ClickHousePushPeriod,
		),
		c.ShutdownTimeout,
	)
	return &App{
		kafka: kafkaClient,
		pipe:  pipe,
	}, nil
}

func (app *App) Stop() {
	app.pipe.Stop()
}

func (app *App) Wait() error {
	defer app.kafka.Close()
	return app.pipe.Wait()
}
