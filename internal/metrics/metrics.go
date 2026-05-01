package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/Achno2k/go-task-queue/internal/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	enqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tasks_enqueued_total",
		Help: "Tasks enqueued by name.",
	}, []string{"name"})

	processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tasks_processed_total",
		Help: "Tasks finished by terminal status (completed, retried, dead_letter).",
	}, []string{"status", "name"})

	duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_duration_seconds",
		Help:    "Wall-clock time spent in the handler.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"name"})

	inflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tasks_inflight",
		Help: "Tasks currently being processed by a worker.",
	})

	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_depth",
		Help: "Length of each Redis queue (sampled).",
	}, []string{"queue"})
)

func TasksEnqueued(name string)          { enqueued.WithLabelValues(name).Inc() }
func TasksProcessed(status, name string) { processed.WithLabelValues(status, name).Inc() }


func ObserveDuration(name string, d time.Duration) {
	duration.WithLabelValues(name).Observe(d.Seconds())
}
func InflightInc() { inflight.Inc() }
func InflightDec() { inflight.Dec() }

// StartDepthSampler periodically polls Redis to keep the queue_depth gauges
// fresh. Cheap — three LLEN/ZCARD calls every five seconds.
func StartDepthSampler(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sample(ctx)
		}
	}
}

func sample(ctx context.Context) {
	if n, err := redis.RDB.LLen(ctx, redis.MainQueueKey).Result(); err == nil {
		queueDepth.WithLabelValues("main").Set(float64(n))
	} else {
		slog.Debug("LLen main failed", "err", err)
	}
	if n, err := redis.RDB.ZCard(ctx, redis.DelayQueueKey).Result(); err == nil {
		queueDepth.WithLabelValues("delay").Set(float64(n))
	}
	if n, err := redis.RDB.LLen(ctx, redis.DLQKey).Result(); err == nil {
		queueDepth.WithLabelValues("dlq").Set(float64(n))
	}
}
