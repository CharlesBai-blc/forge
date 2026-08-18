package burst

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// stubTerraform writes an executable that records its argv, one
// invocation per line, and exits with the given code.
func stubTerraform(t *testing.T, exitCode int) (bin, logPath string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell stub")
	}
	dir := t.TempDir()
	logPath = filepath.Join(dir, "invocations.log")
	bin = filepath.Join(dir, "terraform")
	script := fmt.Sprintf("#!/bin/sh\necho \"$@\" >> %q\nexit %d\n", logPath, exitCode)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, logPath
}

func TestCLIApplyInvocations(t *testing.T) {
	bin, logPath := stubTerraform(t, 0)
	cli := &CLI{
		Bin: bin,
		Dir: "/mod/dir",
		StaticVars: map[string]string{
			"agent_download_url": "https://example/forge-agent",
		},
	}

	if err := cli.Apply(context.Background(), 1, map[string]string{
		"enroll_token":      "tok-1",
		"control_plane_url": "https://cp:8080",
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := cli.Apply(context.Background(), 0, nil); err != nil {
		t.Fatalf("Apply down: %v", err)
	}

	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("invocations = %d, want 3 (init + 2 applies): %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "init") || !strings.Contains(lines[0], "-chdir=/mod/dir") {
		t.Fatalf("first invocation not init: %q", lines[0])
	}
	up := lines[1]
	for _, want := range []string{
		"apply", "-auto-approve", "-var instance_count=1",
		"-var agent_download_url=https://example/forge-agent",
		"-var control_plane_url=https://cp:8080", "-var enroll_token=tok-1",
	} {
		if !strings.Contains(up, want) {
			t.Fatalf("apply missing %q: %q", want, up)
		}
	}
	if !strings.Contains(lines[2], "-var instance_count=0") {
		t.Fatalf("scale-down apply: %q", lines[2])
	}
}

func TestCLIApplyFailureIncludesOutput(t *testing.T) {
	bin, _ := stubTerraform(t, 1)
	cli := &CLI{Bin: bin, Dir: t.TempDir()}
	err := cli.Apply(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("Apply with failing terraform succeeded")
	}
	if !strings.Contains(err.Error(), "terraform init") {
		t.Fatalf("err = %v; want init failure surfaced", err)
	}
}
