package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type workerCred struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

func workerPath(dataDir string) string {
	return filepath.Join(dataDir, "worker.json")
}

func loadWorker(path string) (workerCred, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return workerCred{}, err
	}
	var c workerCred
	if err := json.Unmarshal(b, &c); err != nil {
		return workerCred{}, fmt.Errorf("forge-agent: worker file: %w", err)
	}
	if c.ID == "" || c.Token == "" {
		return workerCred{}, fmt.Errorf("forge-agent: worker file missing id or token")
	}
	return c, nil
}

func saveWorker(path string, c workerCred) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}
