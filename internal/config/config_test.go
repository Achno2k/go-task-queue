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
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("REDIS_ADDR", "redis:6380")
	t.Setenv("DB_DSN", "host=db")

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
	if cfg.DBDSN != "host=db" {
		t.Errorf("DBDSN: got %q", cfg.DBDSN)
	}
	if cfg.HTTPPort != "9090" {
		t.Errorf("HTTPPort: got %q", cfg.HTTPPort)
	}
}

func TestBadIntFallsBack(t *testing.T) {
	t.Setenv("WORKER_COUNT", "not-a-number")
	cfg := Load()
	if cfg.WorkerCount != 3 {
		t.Errorf("expected fallback, got %d", cfg.WorkerCount)
	}
}

func TestPlatformEnvFallbacks(t *testing.T) {
	t.Setenv("DB_DSN", "")
	t.Setenv("DATABASE_URL", "postgres://user:pass@host:5432/db")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_URL", "redis://default:pass@host:6379")
	t.Setenv("HTTP_PORT", "")
	t.Setenv("PORT", "10000")

	cfg := Load()
	if cfg.DBDSN != "postgres://user:pass@host:5432/db" {
		t.Errorf("DBDSN fallback: got %q", cfg.DBDSN)
	}
	if cfg.RedisAddr != "redis://default:pass@host:6379" {
		t.Errorf("RedisAddr fallback: got %q", cfg.RedisAddr)
	}
	if cfg.HTTPPort != "10000" {
		t.Errorf("HTTPPort fallback: got %q", cfg.HTTPPort)
	}
}
