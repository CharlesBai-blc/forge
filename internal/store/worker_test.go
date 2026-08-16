package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func TestEnrollCreatesActiveWorker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatalf("IssueEnrollmentToken: %v", err)
	}
	id, machine, err := s.Enroll(ctx, tok, "host-a", "arm64", "test")
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if id == "" || machine == "" {
		t.Fatal("empty worker id or token")
	}
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if w.Name != "host-a" || w.Arch != "arm64" || w.State != job.WorkerActive || w.Capacity != 1 {
		t.Fatalf("worker = %+v", w)
	}
	got, err := s.WorkerByToken(ctx, machine)
	if err != nil {
		t.Fatalf("WorkerByToken: %v", err)
	}
	if got.ID != id {
		t.Fatalf("WorkerByToken id = %s, want %s", got.ID, id)
	}
}

func TestEnrollSingleUse(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Enroll(ctx, tok, "a", "amd64", "test"); err != nil {
		t.Fatalf("first Enroll: %v", err)
	}
	_, _, err = s.Enroll(ctx, tok, "b", "amd64", "test")
	if err != ErrEnrollmentUsed {
		t.Fatalf("second Enroll = %v, want ErrEnrollmentUsed", err)
	}
}

func TestEnrollExpired(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.issueEnrollmentToken(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Enroll(ctx, tok, "a", "amd64", "test")
	if err != ErrEnrollmentExpired {
		t.Fatalf("Enroll = %v, want ErrEnrollmentExpired", err)
	}
}

func TestEnrollInvalid(t *testing.T) {
	s := openTestStore(t)
	_, _, err := s.Enroll(context.Background(), "no-such-token", "a", "amd64", "test")
	if err != ErrEnrollmentInvalid {
		t.Fatalf("Enroll = %v, want ErrEnrollmentInvalid", err)
	}
}

func TestWorkerByTokenUnknown(t *testing.T) {
	s := openTestStore(t)
	_, err := s.WorkerByToken(context.Background(), "nope")
	if err != ErrWorkerNotFound {
		t.Fatalf("err = %v, want ErrWorkerNotFound", err)
	}
}

func TestEnrollmentAndMachineTokensNotPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forge.db")
	ctx := context.Background()
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, machine, err := s.Enroll(ctx, tok, "a", "amd64", "test")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(tok)) || bytes.Contains(raw, []byte(machine)) {
		t.Fatal("plaintext token found in database file")
	}
}
