package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	// Init logging.
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

	// Build and start the app.
	anonymizer, err := app.New(*c, logger.Named("anonymizer"))
	if err != nil {
		return err
	}

	// Wait for a signal and terminate.
	go func() {
		<-sigCh
		logger.Info("Signal received, terminating...")
		signal.Stop(sigCh)
		anonymizer.Stop()
	}()
	return anonymizer.Wait()
}
