package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/Achno2k/go-task-queue/internal/config"
	"github.com/Achno2k/go-task-queue/internal/redis"
)

// StartReaper watches task_leases for entries whose deadline has passed.
// An expired lease means the worker that held the task is dead (crashed,
// OOM-killed, network partition) — we move the task back onto the main
// queue so another worker can pick it up.
//
// This is the bit that makes the queue "distributed" in any meaningful
// sense: workers can vanish and the system still drains.
func StartReaper(ctx context.Context, cfg config.Config) {
	log := slog.With("component", "reaper")
	log.Info("reaper started", "interval", cfg.ReaperInterval)

	t := time.NewTicker(cfg.ReaperInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("reaper stopping")
			return
		case <-t.C:
			reclaimExpired(ctx, log)
		}
	}
}

func reclaimExpired(ctx context.Context, log *slog.Logger) {
	now := time.Now().Unix()

	leases, err := redis.RDB.HGetAll(ctx, redis.LeaseKey).Result()
	if err != nil {
		log.Error("HGetAll failed", "err", err)
		return
	}

	reclaimed := 0
	for taskID, expStr := range leases {
		exp, err := strconv.ParseInt(expStr, 10, 64)
		if err != nil {
			redis.RDB.HDel(ctx, redis.LeaseKey, taskID)
			continue
		}
		if exp > now {
			continue
		}

		if reclaimOne(ctx, log, taskID) {
			reclaimed++
		}
	}

	if reclaimed > 0 {
		log.Warn("reclaimed expired leases", "count", reclaimed)
	}
}

// reclaimOne walks every processing:* list looking for the task. It's O(N)
// in the number of in-flight tasks, which is fine for a worker count in
// the tens-to-hundreds range. If you scale beyond that, store
// task_id -> processing_key alongside the lease.
func reclaimOne(ctx context.Context, log *slog.Logger, taskID string) bool {
	keys, err := redis.RDB.Keys(ctx, redis.ProcessingPrefix+"*").Result()
	if err != nil {
		log.Error("scan processing lists", "err", err)
		return false
	}

	for _, key := range keys {
		items, err := redis.RDB.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			continue
		}
		for _, raw := range items {
			var probe struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(raw), &probe); err != nil {
				continue
			}
			if probe.ID != taskID {
				continue
			}

			// Found the orphaned task. Move it back to the main queue
			// and clear the lease.
			pipe := redis.RDB.TxPipeline()
			pipe.LRem(ctx, key, 1, raw)
			pipe.LPush(ctx, redis.MainQueueKey, raw)
			pipe.HDel(ctx, redis.LeaseKey, taskID)
			if _, err := pipe.Exec(ctx); err != nil {
				log.Error("requeue pipeline failed", "err", err, "task_id", taskID)
				return false
			}
			log.Warn("requeued orphan task", "task_id", taskID, "from", key)
			return true
		}
	}

	// Lease points at a task we can't find — clean up.
	redis.RDB.HDel(ctx, redis.LeaseKey, taskID)
	return false
}
