package secret

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateGeneratesAndReloads(t *testing.T) {
	dir := t.TempDir()
	k1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	k2, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if *k1 != *k2 {
		t.Fatal("reloaded key differs")
	}
	info, err := os.Stat(filepath.Join(dir, keyFile))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestLoadOrCreateRejectsWrongSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, keyFile), []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreate(dir); err == nil {
		t.Fatal("expected error")
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	k, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("github-token-value")
	box, err := Seal(k, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(box, plain) {
		t.Fatal("ciphertext contains plaintext")
	}
	got, err := Open(k, box)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %q, want %q", got, plain)
	}
}

func TestOpenWrongKey(t *testing.T) {
	a, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	box, err := Seal(a, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(b, box); err == nil {
		t.Fatal("expected decrypt error")
	}
}

func TestOpenTruncated(t *testing.T) {
	k, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(k, []byte("x")); err == nil {
		t.Fatal("expected error")
	}
}
