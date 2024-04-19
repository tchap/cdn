package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/app"
	"github.com/tchap/cdn/anonymizer/anonymizer/internal/config"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	// Load config.
	c, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %+v", err)
		return err
	}

	// Logger could be initialized before loading config right now,
	// but in practice we would load config and configure a logger based on that.
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logging: %+v", err)
		return err
	}
	logger = logger.WithOptions(zap.IncreaseLevel(c.LogLevel))
	defer logger.Sync()

	// Start processing signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

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
		kgo.WithLogger(kzap.New(logger.Named("kafka"))),
	)
	if err != nil {
		logger.Error("Failed to init Kafka client.", zap.Error(err))
		return err
	}
	defer kafkaClient.Close()

	// Run the app.
	anonymizer := app.New(
		logger.Named("anonymizer"),
		kafkaClient,
		c.KafkaTopic,
		c.ClickHouseURL,
		c.ClickHouseTableName,
		c.ClickHousePushPeriod,
		c.ClickHousePushRetryCount,
		c.ShutdownTimeout,
	)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		anonymizer.Stop()
	}()
	return anonymizer.Wait()
}
