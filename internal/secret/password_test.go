package secret

import (
	"bytes"
	"testing"
)

func TestHashAndVerifyPassword(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if bytes.Contains(hash, []byte("correct horse")) {
		t.Fatal("hash contains plaintext")
	}
	if !VerifyPassword(hash, "correct horse battery staple") {
		t.Fatal("correct password rejected")
	}
	if VerifyPassword(hash, "wrong password") {
		t.Fatal("wrong password accepted")
	}
}

func TestHashPasswordSalted(t *testing.T) {
	a, err := HashPassword("same password")
	if err != nil {
		t.Fatal(err)
	}
	b, err := HashPassword("same password")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two hashes of the same password are identical: salt missing")
	}
}

func TestVerifyPasswordMalformedHash(t *testing.T) {
	for _, hash := range [][]byte{nil, {}, []byte("short"), make([]byte, 100)} {
		if VerifyPassword(hash, "anything") {
			t.Fatalf("malformed hash %d bytes accepted", len(hash))
		}
	}
}
