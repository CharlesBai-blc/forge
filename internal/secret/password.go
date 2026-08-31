package secret

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters (RFC 9106 second recommended option: 64 MiB,
// 3 passes). A password hash is compared, not decrypted, so it lives
// here rather than in the secrets table (tdd.md §7).
const (
	argonTime    = 3
	argonMemory  = 64 * 1024 // KiB
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

// HashPassword returns salt || Argon2id key for the admin password (FR-2).
func HashPassword(password string) ([]byte, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("secret: salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return append(salt, key...), nil
}

// VerifyPassword reports whether password matches a HashPassword output.
func VerifyPassword(hash []byte, password string) bool {
	if len(hash) != argonSaltLen+argonKeyLen {
		return false
	}
	key := argon2.IDKey([]byte(password), hash[:argonSaltLen], argonTime, argonMemory, argonThreads, argonKeyLen)
	return subtle.ConstantTimeCompare(key, hash[argonSaltLen:]) == 1
}
