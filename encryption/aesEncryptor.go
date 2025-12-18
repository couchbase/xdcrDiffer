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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

// AES256GCMEncryptor implements EncryptorDecryptor using AES-256-GCM.
type AES256GCMEncryptor struct {
	key          []byte
	nonceCounter uint64
}

// Compile-time check that AES256GCMEncryptor implements EncryptorDecryptor
var _ EncryptorDecryptor = (*AES256GCMEncryptor)(nil)

// NewAES256GCMEncryptor creates a new AES-256-GCM encryptor with the given key.
// The key must be 32 bytes (256 bits).
func NewAES256GCMEncryptor(key []byte) (*AES256GCMEncryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}
	return &AES256GCMEncryptor{
		key: key,
	}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Returns (ciphertext_with_nonce, nonce, error)
func (e *AES256GCMEncryptor) Encrypt(plaintext []byte) ([]byte, []byte, error) {
	if e.key == nil {
		return nil, nil, fmt.Errorf("encryption key not initialized")
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	// The nonce must be unique for each encryption operation with the same key.
	// GCM recommends a 12-byte nonce.
	nonce := make([]byte, gcm.NonceSize())

	// Use an ever-incrementing counter for the first 8 bytes to guarantee uniqueness.
	counter := atomic.AddUint64(&e.nonceCounter, 1)
	binary.BigEndian.PutUint64(nonce[:8], counter)
	// Fill the remaining bytes (if any) with cryptographic randomness.
	if len(nonce) > 8 {
		if _, err = io.ReadFull(rand.Reader, nonce[8:]); err != nil {
			return nil, nil, err
		}
	}

	out := make([]byte, 0, len(nonce)+len(plaintext)+gcm.Overhead())
	out = append(out, nonce...)
	out = gcm.Seal(out, nonce, plaintext, nil)
	return out, nonce, nil
}

// Decrypt decrypts ciphertext using AES-256-GCM.
// preAllocatedBuf can be provided for efficiency; if nil, a new buffer will be allocated.
func (e *AES256GCMEncryptor) Decrypt(ciphertext, nonce, preAllocatedBuf []byte) ([]byte, error) {
	if e.key == nil {
		return nil, fmt.Errorf("encryption key not initialized")
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(preAllocatedBuf, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
