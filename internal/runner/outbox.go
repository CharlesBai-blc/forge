package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/CharlesBai-blc/forge/internal/api"
)

type pendingStatus struct {
	JobID   string           `json:"job_id"`
	Attempt int              `json:"attempt"`
	Report  api.StatusReport `json:"report"`
}

// Outbox is an ordered buffer of status reports. Path empty means memory only.
type Outbox struct {
	path string

	mu    sync.Mutex
	items []pendingStatus
}

// OpenOutbox loads pending reports from path. Missing file is empty.
func OpenOutbox(path string) (*Outbox, error) {
	o := &Outbox{path: path}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return o, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(b, &o.items); err != nil {
		return nil, fmt.Errorf("runner: status outbox: %w", err)
	}
	return o, nil
}

func (o *Outbox) Push(jobID string, attempt int, rep api.StatusReport) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.items = append(o.items, pendingStatus{JobID: jobID, Attempt: attempt, Report: rep})
	return o.persistLocked()
}

func (o *Outbox) Peek() (pendingStatus, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.items) == 0 {
		return pendingStatus{}, false
	}
	return o.items[0], true
}

func (o *Outbox) Pop() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.items) == 0 {
		return nil
	}
	o.items = o.items[1:]
	return o.persistLocked()
}

func (o *Outbox) persistLocked() error {
	if o.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(o.path), 0o700); err != nil {
		return err
	}
	if o.items == nil {
		o.items = []pendingStatus{}
	}
	b, err := json.Marshal(o.items)
	if err != nil {
		return err
	}
	tmp := o.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, o.path)
}
