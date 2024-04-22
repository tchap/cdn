package pipeline

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"gopkg.in/tomb.v2"
)

var ErrSkipRecord = stderrors.New("skip record")

type MessageParser[Record any] interface {
	ParseMessage(*kgo.Record) (Record, error)
}

type RecordTransformer[Record any] interface {
	TransformRecord(Record) Record
}

type AggregationWindow[Record, State any] interface {
	AppendRecord(record Record) error
	Aggregate() State
}

type AggregationWindowConstructor[Record, State any] func() AggregationWindow[Record, State]

type Pusher[AggregationState any] interface {
	Push(ctx context.Context, state AggregationState, windowSize int) error
}

type pushContext[AggregationState any] struct {
	WindowState   AggregationState
	WindowSize    int
	CommitOffsets map[int32]kgo.EpochOffset
}

type Pipeline[MessageRecord, AggregationState any] struct {
	logger               *zap.Logger
	kafka                *kgo.Client
	kafkaTopicName       string
	newAggregationWindow AggregationWindowConstructor[MessageRecord, AggregationState]
	aggregationPeriod    time.Duration
	messageParser        MessageParser[MessageRecord]
	recordTransformer    RecordTransformer[MessageRecord]
	pusher               Pusher[AggregationState]
	shutdownTimeout      time.Duration

	consumerOutputCh chan *kgo.Record
	pusherInputCh    chan pushContext[AggregationState]

	t tomb.Tomb
}

func New[MessageRecord, AggregationState any](
	logger *zap.Logger,
	kafkaClient *kgo.Client,
	kafkaTopicName string,
	messageParser MessageParser[MessageRecord],
	recordTransformer RecordTransformer[MessageRecord],
	newAggregationWindow AggregationWindowConstructor[MessageRecord, AggregationState],
	aggregationPeriod time.Duration,
	pusher Pusher[AggregationState],
	shutdownTimeout time.Duration,
) *Pipeline[MessageRecord, AggregationState] {
	p := &Pipeline[MessageRecord, AggregationState]{
		logger:               logger,
		kafka:                kafkaClient,
		kafkaTopicName:       kafkaTopicName,
		messageParser:        messageParser,
		recordTransformer:    recordTransformer,
		newAggregationWindow: newAggregationWindow,
		aggregationPeriod:    aggregationPeriod,
		pusher:               pusher,
		consumerOutputCh:     make(chan *kgo.Record, 1),
		pusherInputCh:        make(chan pushContext[AggregationState], 1),
	}
	p.t.Go(p.consumerLoop)
	p.t.Go(p.aggregatorLoop)
	p.t.Go(p.pusherLoop)
	return p
}

func (p *Pipeline[MessageRecord, AggregationState]) Stop() {
	p.t.Kill(nil)
}

func (p *Pipeline[MessageRecord, AggregationState]) Wait() error {
	return p.t.Wait()
}

func (p *Pipeline[MessageRecord, AggregationState]) consumerLoop() error {
	logger := p.logger.Named("consumer")
	logger.Info("Consumer starting...", zap.String("kafka_topic", p.kafkaTopicName))
	defer logger.Info("Consumer terminated.")

	ctx := p.t.Context(nil)
	for {
		// Get another batch of records.
		fetches := p.kafka.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil
		}

		fetchFailed := false
		fetches.EachError(func(_ string, _ int32, err error) {
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
			p.t.Kill(err)
		})
		if fetchFailed {
			// We can return nil as the tomb is already killed with the right error.
			return nil
		}

		// Process all records by sending them on the output channel.
		fetches.EachRecord(func(r *kgo.Record) {
			select {
			case p.consumerOutputCh <- r:
			case <-ctx.Done():
				return
			}
		})
	}
}

func (p *Pipeline[MessageRecord, AggregationState]) aggregatorLoop() error {
	logger := p.logger.Named("aggregator")
	logger.Info("Aggregator starting...", zap.Duration("aggregation_period", p.aggregationPeriod))
	defer logger.Info("Aggregator terminated.")

	ctx := p.t.Context(nil)
	ticker := time.NewTicker(p.aggregationPeriod)
	defer ticker.Stop()

	var (
		agg           AggregationWindow[MessageRecord, AggregationState]
		windowSize    int
		commitOffsets map[int32]kgo.EpochOffset
	)
	resetWindow := func() {
		agg = p.newAggregationWindow()
		windowSize = 0
		commitOffsets = make(map[int32]kgo.EpochOffset)
	}
	resetWindow()

	for {
		select {
		case m := <-p.consumerOutputCh:
			// Parse the message.
			r, err := p.messageParser.ParseMessage(m)
			if err != nil {
				if stderrors.Is(err, ErrSkipRecord) {
					continue
				}
				return err
			}

			// Transform, optionally.
			if p.recordTransformer != nil {
				r = p.recordTransformer.TransformRecord(r)
			}

			// Add the record into the aggregation window.
			// Logging is expected to be handled by the aggregator.
			if err := agg.AppendRecord(r); err != nil {
				if stderrors.Is(err, ErrSkipRecord) {
					continue
				}
				return err
			}

			// Update Kafka offsets to be committed.
			commitOffsets[m.Partition] = kgo.EpochOffset{
				Epoch:  m.LeaderEpoch,
				Offset: m.Offset,
			}

			windowSize++

		case <-ticker.C:
			// Do nothing in case there are no records buffered.
			if windowSize == 0 {
				logger.Info("Buffer empty, skipping push...")
				continue
			}

			// Forward to the pusher.
			select {
			case p.pusherInputCh <- pushContext[AggregationState]{
				WindowState:   agg.Aggregate(),
				WindowSize:    windowSize,
				CommitOffsets: commitOffsets,
			}:
				resetWindow()

			case <-ctx.Done():
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (p *Pipeline[MessageRecord, AggregationState]) pusherLoop() error {
	logger := p.logger.Named("pusher")
	logger.Info("Pusher starting...")
	defer logger.Info("Pusher terminated.")

	ctx := p.t.Context(nil)
	for {
		select {
		case push := <-p.pusherInputCh:
			// Push. Set timeout to the aggregation period.
			pushCtx, cancelPush := context.WithTimeout(ctx, p.aggregationPeriod)
			err := p.pusher.Push(pushCtx, push.WindowState, push.WindowSize)
			cancelPush()
			if err != nil {
				return err
			}

			// Commit Kafka offsets. This currently does not have a timeout set.
			logger.Info("Kafka offsets being committed...", zap.Reflect("offsets", push.CommitOffsets))
			commitErrCh := make(chan error, 1)
			p.kafka.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{
				p.kafkaTopicName: push.CommitOffsets,
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

			if err := <-commitErrCh; err != nil {
				// No need to crash really, we will just succeed eventually or do duplicate processing.
				logger.Error(
					"Failed to commit Kafka offsets.",
					zap.Error(err),
				)
				continue
			}

		case <-ctx.Done():
			return nil
		}
	}
}
