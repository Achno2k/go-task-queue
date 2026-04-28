package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Achno2k/go-task-queue/internal/redis"
	r "github.com/redis/go-redis/v9"
)

// StartSchedular promotes delayed tasks (retries, scheduled work) onto the
// main queue once their NextRunAt is in the past. The delay queue is a
// Redis sorted set scored by unix timestamp, so this is just a range scan
// over [-inf, now].
func StartSchedular(ctx context.Context) {
	log := slog.With("component", "scheduler")
	log.Info("scheduler started")

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("scheduler stopping")
			return
		case <-t.C:
			promoteReady(ctx, log)
		}
	}
}

func promoteReady(ctx context.Context, log *slog.Logger) {
	now := time.Now().Unix()

	tasks, err := redis.RDB.ZRangeArgs(ctx, r.ZRangeArgs{
		Key:     redis.DelayQueueKey,
		Start:   "-inf",
		Stop:    fmt.Sprintf("%d", now),
		ByScore: true,
	}).Result()
	if err != nil {
		log.Error("ZRangeArgs failed", "err", err)
		return
	}

	for _, taskJSON := range tasks {
		// Promote: remove from delay set, push to main queue.
		// Done in a pipeline so a crash mid-promote doesn't drop the task.
		pipe := redis.RDB.TxPipeline()
		pipe.ZRem(ctx, redis.DelayQueueKey, taskJSON)
		pipe.LPush(ctx, redis.MainQueueKey, taskJSON)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Error("promote failed", "err", err)
		}
	}
}
