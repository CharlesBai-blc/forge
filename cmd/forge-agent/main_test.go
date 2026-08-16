package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/CharlesBai-blc/forge/internal/api"
)

func TestSaveLoadWorker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.json")
	want := workerCred{ID: "abc", Token: "tok"}
	if err := saveWorker(path, want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0600", info.Mode().Perm())
	}
	got, err := loadWorker(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestResolveClientEnrollsOnce(t *testing.T) {
	var enrolls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agents/enroll" {
			http.NotFound(w, r)
			return
		}
		enrolls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.EnrollResponse{WorkerID: "w1", Token: "machine"})
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	ctx := context.Background()
	c, err := resolveClient(ctx, srv.URL, dir, "enroll-tok")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if c.WorkerID != "w1" || c.Token != "machine" {
		t.Fatalf("client = %+v", c)
	}
	c2, err := resolveClient(ctx, srv.URL, dir, "other-tok")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if c2.WorkerID != "w1" || c2.Token != "machine" {
		t.Fatalf("reload = %+v", c2)
	}
	if enrolls != 1 {
		t.Fatalf("enrolls = %d, want 1", enrolls)
	}
}

func TestResolveClientRequiresEnrollToken(t *testing.T) {
	_, err := resolveClient(context.Background(), "http://127.0.0.1", t.TempDir(), "")
	if err == nil {
		t.Fatal("expected error")
	}
}
