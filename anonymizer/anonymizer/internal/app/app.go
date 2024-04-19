package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"capnproto.org/go/capnp/v3"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"gopkg.in/tomb.v2"

	"github.com/tchap/cdn/anonymizer/anonymizer/internal/logging"
	"github.com/tchap/cdn/anonymizer/anonymizer/internal/messages"
)

const RemoteAddrSegmentReplacement = "X"

type pushContext struct {
	query         string
	rowCount      int
	commitOffsets map[int32]kgo.EpochOffset
}

type App struct {
	logger                   *zap.Logger
	kafka                    *kgo.Client
	kafkaTopicName           string
	clickHouseURL            string
	clickHouseTableName      string
	clickHousePushPeriod     time.Duration
	clickHousePushRetryCount int
	shutdownTimeout          time.Duration

	consumerOutputCh chan *kgo.Record
	pusherInputCh    chan pushContext

	t tomb.Tomb
}

func New(
	logger *zap.Logger,
	kafkaClient *kgo.Client,
	kafkaTopicName string,
	clickHouseURL string,
	clickHouseTableName string,
	clickHousePushPeriod time.Duration,
	clickHousePushRetryCount int,
	shutdownTimeout time.Duration,
) *App {
	app := &App{
		logger:                   logger,
		kafka:                    kafkaClient,
		kafkaTopicName:           kafkaTopicName,
		clickHouseURL:            clickHouseURL,
		clickHouseTableName:      clickHouseTableName,
		clickHousePushPeriod:     clickHousePushPeriod,
		clickHousePushRetryCount: clickHousePushRetryCount,
		consumerOutputCh:         make(chan *kgo.Record, 1),
		pusherInputCh:            make(chan pushContext, 1),
	}
	app.t.Go(app.consumer)
	app.t.Go(app.buffer)
	app.t.Go(app.pusher)
	return app
}

func (app *App) Stop() {
	app.t.Kill(nil)
}

func (app *App) Wait() error {
	return app.t.Wait()
}

func (app *App) consumer() error {
	logger := app.logger.Named("consumer")
	logger.Info("Consumer starting...")
	defer logger.Info("Consumer terminated.")

	ctx := app.t.Context(nil)
	for {
		// Get another batch of records.
		fetches := app.kafka.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil
		}

		fetchFailed := false
		fetches.EachError(func(t string, p int32, err error) {
			{
				// ErrDataLoss is just for information.
				var ex *kgo.ErrDataLoss
				if errors.As(err, &ex) {
					logger.Warn(
						"Data loss error encountered.",
						zap.String("topic", ex.Topic),
						zap.Int32("partition", ex.Partition),
						zap.Error(ex),
					)
				}
			}

			// Just crash for any other error.
			logger.Error("Unrecoverable fetch error encountered.", zap.Error(err))
			fetchFailed = true
			app.t.Kill(err)
		})
		if fetchFailed {
			// We can return nil as the tomb is already killed with the right error.
			return nil
		}

		// Process all records by sending them on the output channel.
		fetches.EachRecord(func(r *kgo.Record) {
			select {
			case app.consumerOutputCh <- r:
			case <-ctx.Done():
				return
			}
		})
	}
}

func (app *App) buffer() error {
	logger := app.logger.Named("buffer")
	logger.Info("Buffer starting...")
	defer logger.Info("Buffer terminated.")

	ctx := app.t.Context(nil)
	ticker := time.NewTicker(app.clickHousePushPeriod)
	defer ticker.Stop()

	var (
		query         bytes.Buffer
		rowCount      int
		commitOffsets map[int32]kgo.EpochOffset
	)
	resetProcessingContext := func() {
		query.Reset()
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", app.clickHouseTableName)

		rowCount = 0
		commitOffsets = make(map[int32]kgo.EpochOffset)
	}
	resetProcessingContext()

	for {
		select {
		case r := <-app.consumerOutputCh:
			// Unmarshal.
			msg, err := capnp.Unmarshal(r.Value)
			if err != nil {
				logger.Warn(
					"Failed to unmarshal capnp message.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}

			// Decode.
			rec, err := messages.ReadRootHttpLogRecord(msg)
			if err != nil {
				logger.Warn(
					"Failed to decode HTTP log record.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}

			// Anonymize.
			remoteAddr, err := rec.RemoteAddr()
			if err != nil {
				logger.Warn(
					"Failed to decode log record remote address.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}
			if i := strings.LastIndex(remoteAddr, "."); i != -1 {
				remoteAddr = remoteAddr[:i+1] + RemoteAddrSegmentReplacement
			}

			// Append to the query.
			cacheStatus, err := rec.CacheStatus()
			if err != nil {
				logger.Warn(
					"Failed to decode log record cache status.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}

			method, err := rec.Method()
			if err != nil {
				logger.Warn(
					"Failed to decode log record method.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}

			u, err := rec.Url()
			if err != nil {
				logger.Warn(
					"Failed to decode log record URL.",
					zap.Object("message", (*logging.KafkaRecord)(r)),
					zap.Error(err),
				)
				continue
			}

			if len(commitOffsets) != 0 {
				query.WriteString(",")
			}
			fmt.Fprintf(
				&query,
				`('%d:%s', %d, %d, %d, %d, %d, '%s', '%s', '%s', '%s')`,
				r.Partition, strconv.FormatInt(r.Offset, 10),
				rec.TimestampEpochMilli(),
				rec.ResourceId(),
				rec.BytesSent(),
				rec.RequestTimeMilli(),
				rec.ResponseStatus(),
				cacheStatus,
				method,
				remoteAddr,
				u,
			)

			commitOffsets[r.Partition] = kgo.EpochOffset{
				Epoch:  r.LeaderEpoch,
				Offset: r.Offset,
			}

			rowCount++

		case <-ticker.C:
			// Do nothing in case there are no rows buffered.
			if rowCount == 0 {
				logger.Info("Output buffer empty, skipping push...")
				continue
			}

			// Forward to the pusher.
			select {
			case app.pusherInputCh <- pushContext{
				query:         query.String(),
				rowCount:      rowCount,
				commitOffsets: commitOffsets,
			}:
				resetProcessingContext()

			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (app *App) pusher() error {
	logger := app.logger.Named("pusher")
	logger.Info("Pusher starting...")
	defer logger.Info("Pusher terminated.")

	client := retryablehttp.NewClient()
	client.RetryMax = app.clickHousePushRetryCount
	client.RetryWaitMax = app.clickHousePushPeriod
	client.ResponseLogHook = func(_ retryablehttp.Logger, resp *http.Response) {
		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			logger.Warn(
				"ClickHouse returned an unexpected status code.",
				zap.String("clickhouse_url", app.clickHouseURL),
				zap.Int("response_status_code", resp.StatusCode),
				zap.ByteString("response_body", body),
			)
		}
	}

	ctx := app.t.Context(nil)

	// Make sure the right table exists in ClickHouse.
	if err := app.ensureClickHouseTableExists(ctx, logger); err != nil {
		return err
	}

	for {
		select {
		case push := <-app.pusherInputCh:
			start := time.Now()

			// Push to ClickHouse.
			reqCtx, cancelReq := context.WithTimeout(ctx, app.clickHousePushPeriod)
			req, err := retryablehttp.NewRequestWithContext(
				reqCtx, http.MethodPost, app.clickHouseURL, strings.NewReader(push.query))
			if err != nil {
				cancelReq()
				logger.Error("Failed to init ClickHouse request.", zap.Error(err))
				return err
			}

			// We need to crash on error since this includes retries.
			// Would be probably better to handle rate limit exceeded more explicitly to wait.
			resp, err := client.Do(req)
			reqDuration := time.Since(start)
			cancelReq()
			if err != nil {
				logger.Error(
					"Failed to post query to ClickHouse.",
					zap.String("clickhouse_url", app.clickHouseURL),
					zap.Error(err),
				)
				return err
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("clickhouse: unexpected status code: %d", resp.StatusCode)
			}

			// Commit.
			logger.Info("Kafka offsets being committed...", zap.Reflect("offsets", push.commitOffsets))
			start = time.Now()
			commitErrCh := make(chan error, 1)
			app.kafka.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{
				app.kafkaTopicName: push.commitOffsets,
			}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				if ctxErr := ctx.Err(); ctxErr != nil {
					commitErrCh <- ctxErr
				} else {
					// kgo returns nil error even when the response signals an issue.
					// The error is logged internally by kgo, though, so we are not missing that.
					// The offset will be hopefully committed next time, we only risk diplicate processing.
					commitErrCh <- err
				}
			})

			commitDuration := time.Since(start)
			if err := <-commitErrCh; err != nil {
				// No need to crash really, we will just succeed eventually or do duplicate processing.
				logger.Error(
					"Failed to commit Kafka offsets.",
					zap.Error(err),
				)
			} else {
				logger.Info(
					"Batch pushed into ClickHouse successfully.",
					zap.Int("row_count", push.rowCount),
					zap.Duration("kafka_commit_time", commitDuration),
					zap.Duration("clickhouse_push_time", reqDuration),
					zap.Duration("total_time", reqDuration+commitDuration),
				)
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (app *App) ensureClickHouseTableExists(ctx context.Context, logger *zap.Logger) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
		ORDER BY uuid
	`, app.clickHouseTableName)

	/*
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
	*/

	reqCtx, cancelReq := context.WithTimeout(ctx, app.clickHousePushPeriod)
	req, err := http.NewRequestWithContext(
		reqCtx, http.MethodPost, app.clickHouseURL, strings.NewReader(query))
	if err != nil {
		cancelReq()
		logger.Error("Failed to init ClickHouse request.", zap.Error(err))
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	cancelReq()
	if err != nil {
		logger.Error(
			"Failed to ensure ClickHouse table exists.",
			zap.String("clickhouse_url", app.clickHouseURL),
			zap.Error(err),
		)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logger.Error(
			"ClickHouse returned an unexpected status code when ensuring table exists.",
			zap.String("clickhouse_url", app.clickHouseURL),
			zap.String("clickhouse_table_name", app.clickHouseTableName),
			zap.Int("response_status_code", resp.StatusCode),
			zap.ByteString("response_body", body),
		)
		return fmt.Errorf("clickhouse: unexpected status code: %d", resp.StatusCode)
	}

	logger.Info(
		"ClickHouse table ensured to exist.",
		zap.String("clickhouse_url", app.clickHouseURL),
		zap.String("clickhouse_table_name", app.clickHouseTableName),
	)
	return nil
}
