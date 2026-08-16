// Package secret encrypts credentials at rest with NaCl secretbox (FR-27).
package secret

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/crypto/nacl/secretbox"
)

const (
	KeySize   = 32
	nonceSize = 24
	keyFile   = "secret.key"
)

// LoadOrCreate returns the 32-byte key at dir/secret.key, generating
// it with mode 0600 if the file does not exist.
func LoadOrCreate(dir string) (*[KeySize]byte, error) {
	path := filepath.Join(dir, keyFile)
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) != KeySize {
			return nil, fmt.Errorf("secret: %s: want %d bytes, got %d", path, KeySize, len(b))
		}
		var k [KeySize]byte
		copy(k[:], b)
		return &k, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("secret: read %s: %w", path, err)
	}

	var k [KeySize]byte
	if _, err := rand.Read(k[:]); err != nil {
		return nil, fmt.Errorf("secret: generate key: %w", err)
	}
	if err := os.WriteFile(path, k[:], 0o600); err != nil {
		return nil, fmt.Errorf("secret: write %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("secret: chmod %s: %w", path, err)
	}
	return &k, nil
}

// Seal encrypts plaintext. The nonce is prepended to the ciphertext.
func Seal(key *[KeySize]byte, plaintext []byte) ([]byte, error) {
	var nonce [nonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("secret: nonce: %w", err)
	}
	return secretbox.Seal(nonce[:], plaintext, &nonce, key), nil
}

// Open decrypts a Seal output.
func Open(key *[KeySize]byte, box []byte) ([]byte, error) {
	if len(box) < nonceSize {
		return nil, fmt.Errorf("secret: ciphertext too short")
	}
	var nonce [nonceSize]byte
	copy(nonce[:], box[:nonceSize])
	out, ok := secretbox.Open(nil, box[nonceSize:], &nonce, key)
	if !ok {
		return nil, fmt.Errorf("secret: decrypt failed")
	}
	return out, nil
}
