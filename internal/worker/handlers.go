package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Handler runs a single task. Handlers should be idempotent — at-least-once
// delivery means the same task may be delivered twice (e.g. if a worker
// crashes after doing the work but before acking).
type Handler func(ctx context.Context, payload []byte) error

var (
	registryMu sync.RWMutex
	registry   = map[string]Handler{}
)

func Register(name string, h Handler) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = h
}

func lookup(name string) (Handler, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	h, ok := registry[name]
	return h, ok
}

// RegisterDefaults wires up a couple of example handlers so the queue does
// real work out of the box. Replace these with whatever your app needs.
func RegisterDefaults() {
	Register("email.send", emailSendHandler)
	Register("image.resize", imageResizeHandler)
	Register("noop", func(ctx context.Context, _ []byte) error { return nil })
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func emailSendHandler(ctx context.Context, payload []byte) error {
	var p emailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("email.send: bad payload: %w", err)
	}
	if p.To == "" {
		return errors.New("email.send: missing 'to'")
	}

	// Pretend to call an SMTP server. The sleep makes metrics interesting.
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("email sent", "to", p.To, "subject", p.Subject)
	return nil
}

type resizePayload struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

func imageResizeHandler(ctx context.Context, payload []byte) error {
	var p resizePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("image.resize: bad payload: %w", err)
	}
	if p.Width <= 0 || p.Height <= 0 {
		return errors.New("image.resize: width and height must be > 0")
	}

	// Stand-in for actual image work. We're proving the registry path,
	// not benchmarking a resize library.
	select {
	case <-time.After(800 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("image resized", "width", p.Width, "height", p.Height)
	return nil
}
