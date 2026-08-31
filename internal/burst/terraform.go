package burst

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sort"
	"sync"
)

// CLI runs the bundled Terraform module (tdd.md §4.10). The terraform
// binary is an external dependency of burst only; the feature is off
// unless configured.
type CLI struct {
	Bin        string            // terraform binary; default "terraform"
	Dir        string            // module directory
	StaticVars map[string]string // install-wide module inputs, such as agent_download_url
	Log        *slog.Logger

	mu       sync.Mutex
	initDone bool
}

func (c *CLI) bin() string {
	if c.Bin != "" {
		return c.Bin
	}
	return "terraform"
}

// Apply runs terraform apply with the desired instance count. Existing
// instances ignore user_data changes (module lifecycle rule), so a new
// enroll_token only reaches instances created by this apply.
func (c *CLI) Apply(ctx context.Context, count int, vars map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initDone {
		if err := c.run(ctx, "init", "-input=false", "-no-color"); err != nil {
			return err
		}
		c.initDone = true
	}
	args := []string{"apply", "-input=false", "-auto-approve", "-no-color",
		"-var", fmt.Sprintf("instance_count=%d", count)}
	merged := make(map[string]string, len(c.StaticVars)+len(vars))
	for k, v := range c.StaticVars {
		merged[k] = v
	}
	for k := range vars {
		merged[k] = vars[k]
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "-var", k+"="+merged[k])
	}
	return c.run(ctx, args...)
}

func (c *CLI) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, c.bin(), append([]string{"-chdir=" + c.Dir}, args...)...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("burst: terraform %s: %w: %s", args[0], err, tail(out.Bytes(), 512))
	}
	return nil
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(bytes.TrimSpace(b))
}
