package queue

// This package previously held an in-memory channel + sync.Cond based queue
// (see git history for priority_queue.go and delayed_queue.go). We migrated
// to Redis as the source of truth so the queue could survive restarts and
// be shared across multiple worker processes. Recovery + scheduler logic
// still live here.
