package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"go.uber.org/zap"
)

type HTTPPostPusher struct {
	logger       *zap.Logger
	url          string
	retryCount   int
	retryMaxWait time.Duration
}

func NewHTTPPostPusher(
	logger *zap.Logger,
	url string,
	retryCount int,
	retryMaxWait time.Duration,
) *HTTPPostPusher {
	return &HTTPPostPusher{
		logger:       logger,
		url:          url,
		retryCount:   retryCount,
		retryMaxWait: retryMaxWait,
	}
}

func (pusher *HTTPPostPusher) Push(ctx context.Context, body *bytes.Buffer, windowSize int) error {
	// Init HTTP client supporting retries.
	client := retryablehttp.NewClient()
	client.RetryMax = pusher.retryCount
	client.RetryWaitMax = pusher.retryMaxWait
	client.Logger = log.New(io.Discard, "", 0)
	client.ResponseLogHook = func(_ retryablehttp.Logger, resp *http.Response) {
		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			pusher.logger.Warn(
				"Destination server returned an unexpected status code.",
				zap.String("destination_url", pusher.url),
				zap.Int("response_status_code", resp.StatusCode),
				zap.ByteString("response_body", body),
			)
		}
	}

	// Push to the destination URL.
	req, err := retryablehttp.NewRequestWithContext(
		ctx, http.MethodPost, pusher.url, body)
	if err != nil {
		pusher.logger.Error("Failed to init HTTP request.", zap.Error(err))
		return err
	}

	// We need to crash on error since this includes retries.
	// Would be probably better to handle rate limit exceeded more explicitly to wait.
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		pusher.logger.Error(
			"Failed to post state.",
			zap.String("destination_url", pusher.url),
			zap.Error(err),
		)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pusher: unexpected status code: %d", resp.StatusCode)
	}

	pusher.logger.Info(
		"Aggregated state pushed successfully.",
		zap.String("destination_url", pusher.url),
		zap.Int("window_size", windowSize),
	)
	return nil
}
