package worker

import (
	"context"
	"errors"
	"testing"
)

func TestRegisterAndLookup(t *testing.T) {
	Register("unit.ok", func(ctx context.Context, _ []byte) error { return nil })

	h, ok := lookup("unit.ok")
	if !ok {
		t.Fatal("expected handler to be registered")
	}
	if err := h(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := lookup("unit.does-not-exist"); ok {
		t.Fatal("expected lookup miss")
	}
}

func TestSafeRunCatchesPanic(t *testing.T) {
	h := func(ctx context.Context, _ []byte) error {
		panic("boom")
	}
	err := safeRun(context.Background(), h, nil)
	if err == nil {
		t.Fatal("expected panic to surface as error")
	}
}

func TestSafeRunReturnsHandlerError(t *testing.T) {
	want := errors.New("nope")
	h := func(ctx context.Context, _ []byte) error { return want }
	if got := safeRun(context.Background(), h, nil); !errors.Is(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEmailHandlerRejectsBadPayload(t *testing.T) {
	if err := emailSendHandler(context.Background(), []byte(`not json`)); err == nil {
		t.Fatal("expected json error")
	}
	if err := emailSendHandler(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("expected missing-to error")
	}
}

func TestImageHandlerRejectsZeroDims(t *testing.T) {
	if err := imageResizeHandler(context.Background(), []byte(`{"width":0,"height":10}`)); err == nil {
		t.Fatal("expected validation error")
	}
}
