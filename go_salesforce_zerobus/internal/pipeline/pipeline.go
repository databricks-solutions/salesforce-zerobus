package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/databricks"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/pubsub"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/sfauth"
)

// Run is the top-level orchestrator. It wires together the Salesforce subscriber,
// Databricks forwarder, and health monitor, coordinating them via errgroup.
func Run(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	// 1. Authenticate with Salesforce
	logger.Info("Authenticating with Salesforce...")
	creds, err := sfauth.Authenticate(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("Salesforce authentication failed: %w", err)
	}
	logger.Info("Salesforce authentication successful",
		"instance_url", creds.InstanceURL,
		"tenant_id", creds.TenantID,
	)

	// 2. Get replay subscription params from Databricks
	logger.Info("Determining subscription start point...")
	replayMgr, err := databricks.NewReplayManager(cfg, logger)
	if err != nil {
		return fmt.Errorf("creating replay manager: %w", err)
	}
	params, err := replayMgr.GetSubscriptionParams(ctx)
	if err != nil {
		return fmt.Errorf("getting subscription params: %w", err)
	}
	logger.Info("Subscription parameters resolved",
		"replay_type", params.ReplayType,
		"replay_id", params.ReplayID,
	)

	// 3. Create bounded event channel (backpressure) and init-ready channel
	eventCh := make(chan *pubsub.CDCEvent, cfg.EventChannelSize)
	readyCh := make(chan struct{})

	// 4. Create subscriber and forwarder
	reauth := func(ctx context.Context) (*sfauth.Credentials, error) {
		return sfauth.Authenticate(ctx, cfg, logger)
	}
	subscriber, err := pubsub.NewSubscriberClient(cfg, creds, logger, reauth)
	if err != nil {
		return fmt.Errorf("creating subscriber client: %w", err)
	}
	forwarder := databricks.NewForwarder(cfg, logger)

	// 5. Launch goroutines with errgroup
	g, gctx := errgroup.WithContext(ctx)

	// Forwarder: initializes stream, closes readyCh, then consumes events
	g.Go(func() error {
		err := forwarder.Run(gctx, eventCh, readyCh)
		if err != nil && gctx.Err() == nil {
			logger.Error("Forwarder exited with error", "error", err)
		}
		return err
	})

	// Subscriber: waits on readyCh, then subscribes and produces events
	g.Go(func() error {
		err := subscriber.Run(gctx, eventCh, readyCh, params.ReplayType, params.ReplayID)
		if err != nil && gctx.Err() == nil {
			logger.Error("Subscriber exited with error", "error", err)
		}
		return err
	})

	// Health monitor
	g.Go(func() error {
		return healthMonitor(gctx, eventCh, forwarder, logger)
	})

	// 6. Wait for all goroutines to complete
	logger.Info("Pipeline started",
		"topic", cfg.Topic(),
		"table", cfg.DBTableName,
		"batch_size", cfg.BatchSize,
		"channel_size", cfg.EventChannelSize,
	)
	err = g.Wait()

	// 7. Cleanup
	logger.Info("Pipeline shutting down, cleaning up resources...")
	subscriber.Close()
	forwarder.Close()

	if err != nil && ctx.Err() != nil {
		// Shutdown was initiated by context cancellation (signal)
		logger.Info("Pipeline stopped gracefully")
		return nil
	}
	return err
}

// healthMonitor periodically checks pipeline health and logs statistics.
func healthMonitor(ctx context.Context, eventCh <-chan *pubsub.CDCEvent, fwd *databricks.Forwarder, logger *slog.Logger) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	reportTicker := time.NewTicker(5 * time.Minute)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			queueDepth := len(eventCh)
			health := fwd.GetHealth()

			if !health.Healthy {
				logger.Warn("Zerobus stream unhealthy",
					"status", health.Status,
					"queue_depth", queueDepth,
				)
			}

			if queueDepth > 100 {
				logger.Warn("Event queue backing up", "queue_depth", queueDepth)
			}
		case <-reportTicker.C:
			queueDepth := len(eventCh)
			health := fwd.GetHealth()
			logger.Info("Pipeline health report",
				"queue_depth", queueDepth,
				"stream_status", health.Status,
				"stream_healthy", health.Healthy,
			)
		}
	}
}
