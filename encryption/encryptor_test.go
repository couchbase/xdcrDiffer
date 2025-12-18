// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package encryption

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func TestNewAES256GCMEncryptor_InvalidKeyLength(t *testing.T) {
	tests := []struct {
		name    string
		keyLen  int
		wantErr bool
	}{
		{"too short", 16, true},
		{"too long", 64, true},
		{"exact 32 bytes", 32, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := make([]byte, tt.keyLen)
			enc, err := NewAES256GCMEncryptor(key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, enc)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, enc)
			}
		})
	}
}

func TestAES256GCMEncryptor_EncryptDecrypt_RoundTrip(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("secret data payload")
	ciphertextWithNonce, nonce, err := enc.Encrypt(plaintext)
	assert.NoError(t, err)
	assert.NotNil(t, nonce)
	assert.NotEmpty(t, ciphertextWithNonce)

	// Ciphertext should be different from plaintext
	assert.NotEqual(t, plaintext, ciphertextWithNonce)

	// Extract ciphertext (after nonce)
	actualCiphertext := ciphertextWithNonce[len(nonce):]

	// Decrypt
	decrypted, err := enc.Decrypt(actualCiphertext, nonce, nil)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext, decrypted))
}

func TestAES256GCMEncryptor_Encrypt_UniqueNonces(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("same plaintext")
	nonces := make(map[string]bool)

	// Encrypt same plaintext multiple times
	for i := 0; i < 100; i++ {
		_, nonce, err := enc.Encrypt(plaintext)
		assert.NoError(t, err)
		nonceHex := hex.EncodeToString(nonce)
		assert.False(t, nonces[nonceHex], "duplicate nonce detected")
		nonces[nonceHex] = true
	}
}

func TestAES256GCMEncryptor_Decrypt_TamperedCiphertext(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("secret payload")
	ciphertextWithNonce, nonce, err := enc.Encrypt(plaintext)
	assert.NoError(t, err)

	// Tamper with ciphertext
	actualCiphertext := append([]byte{}, ciphertextWithNonce[len(nonce):]...)
	actualCiphertext[0] ^= 0xFF

	// Decrypt should fail
	_, err = enc.Decrypt(actualCiphertext, nonce, nil)
	assert.Error(t, err)
}

func TestAES256GCMEncryptor_Decrypt_WrongNonce(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("another secret")
	ciphertextWithNonce, nonce, err := enc.Encrypt(plaintext)
	assert.NoError(t, err)

	// Use wrong nonce
	badNonce := append([]byte{}, nonce...)
	badNonce[0] ^= 0xFF

	actualCiphertext := ciphertextWithNonce[len(nonce):]
	_, err = enc.Decrypt(actualCiphertext, badNonce, nil)
	assert.Error(t, err)
}

func TestAES256GCMEncryptor_Decrypt_WithPreallocatedBuffer(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("roundtrip plaintext using buffer")
	ciphertextWithNonce, nonce, err := enc.Encrypt(plaintext)
	assert.NoError(t, err)

	actualCiphertext := ciphertextWithNonce[len(nonce):]

	// Preallocate buffer
	buffer := make([]byte, 0, len(plaintext))
	decrypted, err := enc.Decrypt(actualCiphertext, nonce, buffer)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext, decrypted))
}

func TestAES256GCMEncryptor_Decrypt_WithPrefixedBuffer(t *testing.T) {
	key := randomBytes(t, 32)
	enc, err := NewAES256GCMEncryptor(key)
	assert.NoError(t, err)

	plaintext := []byte("buffer dst append test")
	ciphertextWithNonce, nonce, err := enc.Encrypt(plaintext)
	assert.NoError(t, err)

	actualCiphertext := ciphertextWithNonce[len(nonce):]

	// Prefilled buffer - gcm.Open appends to dst
	prefix := []byte("PREFIX-")
	buffer := make([]byte, len(prefix), len(prefix)+len(plaintext))
	copy(buffer, prefix)

	decrypted, err := enc.Decrypt(actualCiphertext, nonce, buffer)
	assert.NoError(t, err)

	// Prefix should remain at start
	assert.True(t, bytes.HasPrefix(decrypted, prefix))
	// Plaintext should follow prefix
	suffix := decrypted[len(prefix):]
	assert.True(t, bytes.Equal(plaintext, suffix))
}

func TestAES256GCMEncryptor_NilKey(t *testing.T) {
	enc := &AES256GCMEncryptor{key: nil}

	_, _, err := enc.Encrypt([]byte("test"))
	assert.Error(t, err)

	_, err = enc.Decrypt([]byte("test"), []byte("nonce12byte!"), nil)
	assert.Error(t, err)
}

func TestDeriveKeyPBKDF2(t *testing.T) {
	passphrase := []byte("correct horse battery staple")
	salt := []byte("1234567890abcdef") // 16 bytes

	key := DeriveKeyPBKDF2(passphrase, salt, 5000, 32)
	assert.Equal(t, 32, len(key))

	// Same inputs should produce same key
	key2 := DeriveKeyPBKDF2(passphrase, salt, 5000, 32)
	assert.True(t, bytes.Equal(key, key2))

	// Different salt should produce different key
	key3 := DeriveKeyPBKDF2(passphrase, []byte("different_salt16"), 5000, 32)
	assert.False(t, bytes.Equal(key, key3))

	// Different iterations should produce different key
	key4 := DeriveKeyPBKDF2(passphrase, salt, 6000, 32)
	assert.False(t, bytes.Equal(key, key4))
}

func TestGenerateSalt(t *testing.T) {
	// Test correct length generation
	salt, err := GenerateSalt(SaltLen)
	assert.NoError(t, err)
	assert.Equal(t, SaltLen, len(salt))

	// Test different lengths
	shortSalt, err := GenerateSalt(8)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(shortSalt))

	longSalt, err := GenerateSalt(32)
	assert.NoError(t, err)
	assert.Equal(t, 32, len(longSalt))
}

func TestCalibrateIterations(t *testing.T) {
	passphrase := []byte("test passphrase")
	salt, _ := GenerateSalt(SaltLen)

	iter := CalibrateIterations(passphrase, salt, 32, CalibrationBudget, TargetDerivationTime)
	assert.Greater(t, iter, 0)

	// Should be at least 1000 (minimum)
	assert.GreaterOrEqual(t, iter, 1000)
}
