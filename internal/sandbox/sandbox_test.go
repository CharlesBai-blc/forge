package sandbox

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

type fakeSandbox struct {
	id      string
	started bool
	dead    bool
}

func (s *fakeSandbox) ID() string { return s.id }

func (s *fakeSandbox) Start(context.Context, string) error {
	if s.dead {
		return fmt.Errorf("sandbox: destroyed")
	}
	if s.started {
		return fmt.Errorf("sandbox: already started")
	}
	s.started = true
	return nil
}

func (s *fakeSandbox) Wait(context.Context) (int, error) {
	if !s.started {
		return 0, fmt.Errorf("sandbox: not started")
	}
	return 0, nil
}

func (s *fakeSandbox) Logs(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *fakeSandbox) Destroy(context.Context) error {
	s.dead = true
	return nil
}

type fakeProvider struct{}

func (fakeProvider) Create(context.Context, Spec) (Sandbox, error) {
	return &fakeSandbox{id: "fake-1"}, nil
}

var (
	_ Provider = fakeProvider{}
	_ Sandbox  = (*fakeSandbox)(nil)
)

func TestStartTwiceErrors(t *testing.T) {
	sb, err := fakeProvider{}.Create(context.Background(), Spec{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sb.Start(context.Background(), ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sb.Start(context.Background(), ""); err == nil {
		t.Fatal("expected error on second Start")
	}
}

func TestDestroyIdempotent(t *testing.T) {
	sb, err := fakeProvider{}.Create(context.Background(), Spec{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sb.Destroy(context.Background()); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if err := sb.Destroy(context.Background()); err != nil {
		t.Fatalf("second Destroy: %v", err)
	}
}
