// Package obs holds Prometheus metrics, logging setup, and the admin HTTP
// server. Metric labels are deliberately low-cardinality: never label hot
// metrics by tenant or org (thousands of values); use /debug/subscriptions for
// per-subscription detail instead.
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process-wide metrics registry served on /metrics.
var Registry = prometheus.NewRegistry()

var (
	Subscriptions = gaugeVec("sfzb_subscriptions", "Owned subscriptions by state.", "state")
	OwnedTenants  = gauge("sfzb_owned_tenants", "Tenants owned by this shard.")
	ShardInfo     = gaugeVec("sfzb_shard_info", "Shard assignment of this replica (value is always 1).", "index", "count")

	EventsReceived   = counterVec("sfzb_events_received_total", "Events received from Salesforce.", "table")
	EventsSubmitted  = counterVec("sfzb_events_submitted_total", "Events handed to the sink.", "table")
	EventsAcked      = counterVec("sfzb_events_acked_total", "Events durably acknowledged by Zerobus.", "table")
	EventsDuplicate  = counterVec("sfzb_events_duplicate_total", "Redelivered events dropped by the dedup cache.", "table")
	DecodeErrors     = counterVec("sfzb_decode_errors_total", "Events written raw because the payload could not be decoded.", "table")
	RecordsTruncated = counterVec("sfzb_records_truncated_total", "Rows truncated to fit the Zerobus payload limit.", "table")
	EventE2ESeconds  = histogramVec("sfzb_event_e2e_seconds", "Latency from Salesforce receive to Zerobus ack.",
		[]float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}, "table")

	ZerobusStreams        = gaugeVec("sfzb_zerobus_streams", "Zerobus stream slots by state.", "table", "state")
	ZerobusStreamFailures = counterVec("sfzb_zerobus_stream_failures_total", "Zerobus stream slot failures.", "table")
	ZerobusBatchRecords   = histogramVec("sfzb_zerobus_batch_records", "Records per Zerobus ingest batch.",
		[]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000}, "table")

	CheckpointCommitSeconds = histogram("sfzb_checkpoint_commit_seconds", "Checkpoint batch commit latency.",
		[]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5})
	CheckpointRows          = counter("sfzb_checkpoint_rows_total", "Checkpoint rows written.")
	CheckpointErrors        = counter("sfzb_checkpoint_errors_total", "Failed checkpoint commits.")
	CheckpointDirty         = gauge("sfzb_checkpoint_dirty", "Subscriptions with acked progress not yet committed.")
	CheckpointOldestSeconds = gauge("sfzb_checkpoint_oldest_uncommitted_seconds", "Age of the oldest uncommitted acked progress.")
	CheckpointOrgMismatch   = counter("sfzb_checkpoint_org_mismatch_total", "Stored checkpoints ignored because the org ID changed.")

	SFRPCErrors         = counterVec("sfzb_sf_rpc_errors_total", "Salesforce Pub/Sub RPC errors.", "op", "code")
	SFAuth              = counterVec("sfzb_sf_auth_total", "Salesforce authentications.", "result")
	SubscriptionRestart = counterVec("sfzb_subscription_restarts_total", "Subscription restarts by error class.", "reason")
	ReplayFallback      = counterVec("sfzb_replay_fallback_total", "Expired replay IDs replaced by a preset.", "preset")
	Panics              = counter("sfzb_panics_total", "Recovered panics in subscription runners.")

	SchemaCacheBytes  = gauge("sfzb_schema_cache_bytes", "Bytes held by the Avro schema cache.")
	SchemaCacheHits   = counter("sfzb_schema_cache_hits_total", "Avro schema cache hits.")
	SchemaCacheMisses = counter("sfzb_schema_cache_misses_total", "Avro schema cache misses (GetSchema RPCs).")

	RateLimitWait = histogramVec("sfzb_ratelimit_wait_seconds", "Time spent waiting on rate limiters.",
		[]float64{.001, .01, .1, .5, 1, 5, 15, 30, 60, 120}, "limiter")
	StreamOpenSeconds = histogramVec("sfzb_grpc_stream_open_seconds", "Latency to open gRPC streams.",
		[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}, "target")
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

func counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	Registry.MustRegister(c)
	return c
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	Registry.MustRegister(g)
	return g
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

func histogram(name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	Registry.MustRegister(h)
	return h
}

func histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	Registry.MustRegister(h)
	return h
}
