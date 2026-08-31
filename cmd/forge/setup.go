package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/secret"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/source/github"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// credSource defers GitHub source construction until credentials exist:
// from flags or the store at startup, or from the web setup flow later
// (FR-2). Until then webhook and JIT calls fail with errSetupPending.
type credSource struct {
	mu  sync.RWMutex
	src source.RunnerSource
}

var errSetupPending = errors.New("forge: setup pending: GitHub credentials not configured")

func (c *credSource) get() source.RunnerSource {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.src
}

// Set installs the real source once credentials are available.
func (c *credSource) Set(s source.RunnerSource) {
	c.mu.Lock()
	c.src = s
	c.mu.Unlock()
}

func (c *credSource) VerifyAndParse(r *http.Request) ([]source.JobEvent, error) {
	if s := c.get(); s != nil {
		return s.VerifyAndParse(r)
	}
	return nil, errSetupPending
}

func (c *credSource) RegisterJIT(ctx context.Context, j *job.Job) (*source.JITConfig, error) {
	if s := c.get(); s != nil {
		return s.RegisterJIT(ctx, j)
	}
	return nil, errSetupPending
}

func (c *credSource) Unregister(ctx context.Context, runnerID int64) error {
	if s := c.get(); s != nil {
		return s.Unregister(ctx, runnerID)
	}
	return errSetupPending
}

func (c *credSource) ListQueued(ctx context.Context) ([]source.JobEvent, error) {
	if s := c.get(); s != nil {
		return s.ListQueued(ctx)
	}
	return nil, errSetupPending
}

// seedAdmin creates the admin account from flags or env for scripted
// installs (FR-2). A no-op when unset or when an admin already exists.
func seedAdmin(ctx context.Context, st *store.Store, username, password string) error {
	if username == "" || password == "" {
		return nil
	}
	if len(password) < 8 {
		return fmt.Errorf("forge: -admin-password must be at least 8 characters")
	}
	hash, err := secret.HashPassword(password)
	if err != nil {
		return err
	}
	err = st.CreateAdmin(ctx, username, hash)
	if errors.Is(err, store.ErrAdminExists) {
		return nil
	}
	return err
}

// saveCreds is the web setup flow's persistence hook: it seals the
// provided credentials, then installs the GitHub source on the holder
// once both are known (FR-2, FR-27).
func saveCreds(st *store.Store, key *[secret.KeySize]byte, cfg config, holder *credSource) func(ctx context.Context, webhookSecret, githubToken string) error {
	return func(ctx context.Context, webhookSecret, githubToken string) error {
		if webhookSecret != "" {
			if err := putSealed(ctx, st, key, secretWebhook, webhookSecret); err != nil {
				return err
			}
			cfg.webhookSecret = webhookSecret
		}
		if githubToken != "" {
			if err := putSealed(ctx, st, key, secretToken, githubToken); err != nil {
				return err
			}
			cfg.githubToken = githubToken
		}
		// Fill gaps from previously stored values, then activate.
		full, err := resolveCreds(ctx, st, key, cfg)
		if err != nil {
			return err
		}
		if full.webhookSecret == "" || full.githubToken == "" {
			return fmt.Errorf("forge: both GitHub token and webhook secret are required")
		}
		holder.Set(&github.Source{
			Secret: full.webhookSecret,
			Token:  full.githubToken,
			Owner:  full.githubOwner,
			Repo:   full.githubRepo,
			Org:    full.githubOrg,
		})
		return nil
	}
}

func putSealed(ctx context.Context, st *store.Store, key *[secret.KeySize]byte, name, value string) error {
	box, err := secret.Seal(key, []byte(value))
	if err != nil {
		return err
	}
	return st.PutSecret(ctx, name, box)
}
