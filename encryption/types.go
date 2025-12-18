// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package encryption

// Encryptor defines the interface for encrypting data.
type Encryptor interface {
	// Encrypt takes plaintext and returns encrypted ciphertext along with a nonce.
	// The nonce is typically prepended to the ciphertext in the returned byte slice.
	// Returns (ciphertext_with_nonce, nonce, error)
	Encrypt(plaintext []byte) ([]byte, []byte, error)
}

// Decryptor defines the interface for decrypting data.
type Decryptor interface {
	// Decrypt takes ciphertext and nonce, returns the original plaintext.
	// preAllocatedBuf can be provided for efficiency; if nil, a new buffer will be allocated.
	Decrypt(ciphertext, nonce, preAllocatedBuf []byte) ([]byte, error)
}

// EncryptorDecryptor combines both encryption and decryption capabilities.
type EncryptorDecryptor interface {
	Encryptor
	Decryptor
}

// Config holds configuration parameters for encryption operations.
type PBKDF2Config struct {
	// Salt is the cryptographic salt used in key derivation
	Salt []byte
	// Iteration is the number of PBKDF2 iterations used in key derivation
	Iteration uint64
	// Key is the derived encryption key
	Key []byte
}

// Clone creates a deep copy of the Config.
func (c *PBKDF2Config) Clone() *PBKDF2Config {
	if c == nil {
		return nil
	}
	clone := &PBKDF2Config{
		Iteration: c.Iteration,
	}
	if c.Salt != nil {
		clone.Salt = make([]byte, len(c.Salt))
		copy(clone.Salt, c.Salt)
	}
	if c.Key != nil {
		clone.Key = make([]byte, len(c.Key))
		copy(clone.Key, c.Key)
	}
	return clone
}

// PassphraseGetter is a function type for obtaining passphrases interactively.
type PassphraseGetter func() ([]byte, func(), error)
