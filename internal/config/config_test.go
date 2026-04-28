package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("WORKER_COUNT", "")

	cfg := Load()
	if cfg.WorkerCount != 3 {
		t.Errorf("WorkerCount default: got %d, want 3", cfg.WorkerCount)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("MaxAttempts default: got %d, want 3", cfg.MaxAttempts)
	}
	if cfg.LeaseTimeout != 30*time.Second {
		t.Errorf("LeaseTimeout default: got %v, want 30s", cfg.LeaseTimeout)
	}
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("WORKER_COUNT", "12")
	t.Setenv("MAX_ATTEMPTS", "5")
	t.Setenv("LEASE_TIMEOUT", "60")
	t.Setenv("REDIS_ADDR", "redis:6380")

	cfg := Load()
	if cfg.WorkerCount != 12 {
		t.Errorf("WorkerCount: got %d", cfg.WorkerCount)
	}
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts: got %d", cfg.MaxAttempts)
	}
	if cfg.LeaseTimeout != 60*time.Second {
		t.Errorf("LeaseTimeout: got %v", cfg.LeaseTimeout)
	}
	if cfg.RedisAddr != "redis:6380" {
		t.Errorf("RedisAddr: got %q", cfg.RedisAddr)
	}
}

func TestBadIntFallsBack(t *testing.T) {
	t.Setenv("WORKER_COUNT", "not-a-number")
	cfg := Load()
	if cfg.WorkerCount != 3 {
		t.Errorf("expected fallback, got %d", cfg.WorkerCount)
	}
}
