package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func testStream(t *testing.T) *Stream {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := Open(context.Background(), mr.Addr())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAddClaimAck(t *testing.T) {
	s := testStream(t)
	ctx := context.Background()
	if err := s.Add(ctx, "job-1"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	msg, err := s.Claim(ctx, "w1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if msg.JobID != "job-1" {
		t.Fatalf("JobID = %q", msg.JobID)
	}
	if err := s.Ack(ctx, msg.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestClaimTimeoutEmpty(t *testing.T) {
	s := testStream(t)
	_, err := s.Claim(context.Background(), "w1", 20*time.Millisecond)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("err = %v, want ErrEmpty", err)
	}
}

func TestPendingClaimAfterNoAck(t *testing.T) {
	s := testStream(t)
	ctx := context.Background()
	if err := s.Add(ctx, "job-1"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	first, err := s.Claim(ctx, "w1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	again, err := s.Claim(ctx, "w1", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("pending Claim: %v", err)
	}
	if again.JobID != "job-1" || again.ID != first.ID {
		t.Fatalf("pending = %+v, want %+v", again, first)
	}
}

func TestReconcileAddsMissing(t *testing.T) {
	s := testStream(t)
	ctx := context.Background()
	if err := s.Add(ctx, "job-1"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Reconcile(ctx, []string{"job-1", "job-2"}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ok, err := s.Has(ctx, "job-2")
	if err != nil || !ok {
		t.Fatalf("Has job-2 = %v %v", ok, err)
	}
	first, err := s.Claim(ctx, "w1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if first.JobID != "job-1" {
		t.Fatalf("w1 claim = %s, want job-1", first.JobID)
	}
	second, err := s.Claim(ctx, "w2", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Claim w2: %v", err)
	}
	if second.JobID != "job-2" {
		t.Fatalf("w2 claim = %s, want job-2", second.JobID)
	}
}

func TestAutoClaimIdlePending(t *testing.T) {
	s := testStream(t)
	ctx := context.Background()
	if err := s.Add(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, "w1", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	got, err := s.AutoClaim(ctx, 0)
	if err != nil {
		t.Fatalf("AutoClaim: %v", err)
	}
	if len(got) != 1 || got[0].JobID != "job-1" {
		t.Fatalf("AutoClaim = %+v", got)
	}
	if err := s.Ack(ctx, got[0].ID); err != nil {
		t.Fatal(err)
	}
	again, err := s.AutoClaim(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("after ack AutoClaim = %+v", again)
	}
}
