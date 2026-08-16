package docker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/errdefs"

	"github.com/CharlesBai-blc/forge/internal/sandbox"
)

const testImage = "alpine:3.20"

func dockerProvider(t *testing.T) *Provider {
	t.Helper()
	p, err := NewProvider()
	if err != nil {
		t.Skipf("docker client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := p.cli.Ping(ctx); err != nil {
		p.Close()
		t.Skipf("docker daemon: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func testSpec(cmd ...string) sandbox.Spec {
	return sandbox.Spec{
		Image:   testImage,
		Command: cmd,
	}
}

func createSandbox(t *testing.T, p *Provider, spec sandbox.Spec) sandbox.Sandbox {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	sb, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() {
		_ = sb.Destroy(context.Background())
	})
	return sb
}

func TestCreateRequiresImage(t *testing.T) {
	p := dockerProvider(t)
	_, err := p.Create(context.Background(), sandbox.Spec{})
	if err == nil {
		t.Fatal("expected error for empty image")
	}
}

func TestStartTwiceErrors(t *testing.T) {
	p := dockerProvider(t)
	sb := createSandbox(t, p, testSpec("true"))
	ctx := context.Background()
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sb.Start(ctx); err == nil {
		t.Fatal("expected error on second Start")
	}
}

func TestDestroyIdempotent(t *testing.T) {
	p := dockerProvider(t)
	sb := createSandbox(t, p, testSpec("true"))
	ctx := context.Background()
	if err := sb.Destroy(ctx); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if err := sb.Destroy(ctx); err != nil {
		t.Fatalf("second Destroy: %v", err)
	}
}

func TestDestroyRemovesContainer(t *testing.T) {
	p := dockerProvider(t)
	sb := createSandbox(t, p, testSpec("true"))
	id := sb.ID()
	ctx := context.Background()
	if err := sb.Destroy(ctx); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	_, err := p.cli.ContainerInspect(ctx, id)
	if err == nil {
		t.Fatal("container still present after Destroy")
	}
	if !errdefs.IsNotFound(err) {
		t.Fatalf("inspect after Destroy: %v", err)
	}
}

func TestWaitExitCode(t *testing.T) {
	p := dockerProvider(t)
	sb := createSandbox(t, p, testSpec("sh", "-c", "exit 7"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	code, err := sb.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if code != 7 {
		t.Errorf("exit code = %d, want 7", code)
	}
}

func TestLogs(t *testing.T) {
	p := dockerProvider(t)
	sb := createSandbox(t, p, testSpec("echo", "hello-forge"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	rc, err := sb.Logs(ctx)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer rc.Close()

	waitc := make(chan error, 1)
	go func() {
		_, err := sb.Wait(ctx)
		waitc <- err
	}()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}
	if !bytes.Contains(b, []byte("hello-forge")) {
		t.Errorf("logs = %q, want hello-forge", b)
	}
	if err := <-waitc; err != nil {
		t.Fatalf("Wait: %v", err)
	}
}
