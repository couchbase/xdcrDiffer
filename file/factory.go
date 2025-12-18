// Copyright (c) 2025 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package file

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/couchbase/xdcrDiffer/encryption"
	"github.com/couchbase/xdcrDiffer/utils"
)

// Factory creates File instances (either PlainFile or EncryptedFile).
type Factory struct {
	// encryptionEnabled denotes if encryption is enabled.
	encryptionEnabled bool
	// sessionSalt is the salt used to derive the key for.
	sessionSalt []byte
	// sessionIteration is the iteration count used to derive the key.
	sessionIteration uint64
	// passphraseGetter is used to get the passphras.
	passphraseGetter encryption.PassphraseGetter
	// mtx is used to synchronize access to the encryptor cache.
	mtx sync.RWMutex
	// encryptorCache is a map of salt (hex-encoded) to encryptor instance.
	encryptorCache map[string]*encryption.AES256GCMEncryptor
	// initialized denotes if the factory has been initialized for encryption.
	initialized bool
}

// NewFactory creates a Factory.
// If encryptionEnabled is false, passphraseGetter can be nil.
func NewFactory(encryptionEnabled bool, passphraseGetter encryption.PassphraseGetter) *Factory {
	f := &Factory{
		encryptionEnabled: encryptionEnabled,
		passphraseGetter:  passphraseGetter,
	}
	if encryptionEnabled {
		f.encryptorCache = make(map[string]*encryption.AES256GCMEncryptor)
	}
	return f
}

// InitEncryption initializes the factory for encryption.
// Must be called before OpenFile if encryption is enabled.
func (f *Factory) InitEncryption(decryptMode bool, passphrase []byte) error {
	if !f.encryptionEnabled {
		return nil
	}

	if f.initialized {
		return fmt.Errorf("factory already initialized")
	}

	if decryptMode {
		// In decrypt mode, the passphrase is nil.
		// Key derivation will be performed on-demand during file reads.
		// Mark InitDone and return.
		f.initialized = true
		return nil
	}

	// Generate session salt
	salt, err := encryption.GenerateSalt(encryption.SaltLen)
	if err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}

	// Calibrate iteration count for this hardware
	iteration := encryption.CalibrateIterations(passphrase, salt, encryption.KeyLen, encryption.CalibrationBudget, encryption.TargetDerivationTime)
	if iteration == 0 {
		return fmt.Errorf("calibration returned 0 iterations")
	}

	// Derive key from passphrase
	key := encryption.DeriveKeyPBKDF2(passphrase, salt, iteration, encryption.KeyLen)

	// Create session encryptor
	encryptor, err := encryption.NewAES256GCMEncryptor(key)
	if err != nil {
		// Zero the key on failure
		utils.ZeroBytes(key)
		return fmt.Errorf("failed to create encryptor: %w", err)
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.sessionSalt = salt
	f.sessionIteration = uint64(iteration)
	f.initialized = true

	// Cache session encryptor
	saltHex := hex.EncodeToString(salt)
	f.encryptorCache[saltHex] = encryptor

	return nil
}

// OpenFile opens a file with the specified flags, permissions, and mode.
// For encrypted files, it handles headers automatically:
//   - If creating (new/empty file): uses session salt, writes header
//   - If opening existing: reads header, uses matching encryptor (or derives new key)
func (f *Factory) OpenFile(path string, flag int, perm os.FileMode, mode FileMode) (File, error) {
	if !f.encryptionEnabled {
		return openPlainFile(path, flag, perm)
	}

	if !f.initialized {
		return nil, fmt.Errorf("factory not initialized - call Init() first")
	}

	return f.openEncryptedFile(path, flag, perm, mode)
}

// IsEncrypted returns true if encryption is enabled.
func (f *Factory) IsEncrypted() bool {
	return f.encryptionEnabled
}

// GetSuffix returns the filename suffix for files
func (f *Factory) GetSuffix() string {
	if f.encryptionEnabled {
		return encryption.EncSuffix
	}
	return ""
}

// IsInitialized returns true if Init has been called successfully.
func (f *Factory) IsInitialized() bool {
	return f.initialized
}

// openPlainFile opens a plain (unencrypted) file.
func openPlainFile(path string, flag int, perm os.FileMode) (*PlainFile, error) {
	fd, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &PlainFile{file: fd, name: path}, nil
}

func (f *Factory) openEncryptedFile(path string, flag int, perm os.FileMode, mode FileMode) (*EncryptedFile, error) {
	fd, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	// Determine if file is empty AFTER open
	end, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		fd.Close()
		return nil, err
	}

	var encryptor *encryption.AES256GCMEncryptor

	if end == 0 {
		// New or truncated file - use session salt/encryptor, write header
		encryptor, err = f.setupNewFile(fd)
	} else {
		// Existing file - read header, get appropriate encryptor
		encryptor, err = f.setupExistingFile(fd)
	}

	if err != nil {
		fd.Close()
		return nil, err
	}

	return newEncryptedFile(fd, path, encryptor, mode), nil
}

// setupNewFile writes the session header and returns the session encryptor.
func (f *Factory) setupNewFile(fd *os.File) (*encryption.AES256GCMEncryptor, error) {
	// Write header with session salt and iteration
	if err := f.writeHeader(fd, f.sessionSalt, f.sessionIteration); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	f.mtx.RLock()
	defer f.mtx.RUnlock()

	encryptor, exists := f.encryptorCache[hex.EncodeToString(f.sessionSalt)]
	if !exists {
		return nil, errors.New("encryptor not found in cache")
	}
	return encryptor, nil
}

// setupExistingFile reads the header and returns the appropriate encryptor.
// If the salt matches session salt, returns session encryptor.
// Otherwise, derives a new key using passphraseGetter.
func (f *Factory) setupExistingFile(fd *os.File) (*encryption.AES256GCMEncryptor, error) {
	salt, iteration, err := f.readHeader(fd)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	saltHex := hex.EncodeToString(salt)

	// Fast path: check cache (includes session encryptor)
	f.mtx.RLock()
	if enc, ok := f.encryptorCache[saltHex]; ok {
		f.mtx.RUnlock()
		return enc, nil
	}
	f.mtx.RUnlock()

	// Slow path: different salt, need to derive key
	return f.deriveAndCacheEncryptor(salt, iteration)
}

// deriveAndCacheEncryptor derives a key for a non-session salt and caches the encryptor.
// This is used when reading files from other sessions.
func (f *Factory) deriveAndCacheEncryptor(salt []byte, iteration uint64) (*encryption.AES256GCMEncryptor, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	saltHex := hex.EncodeToString(salt)

	// Double-check after acquiring write lock
	if enc, ok := f.encryptorCache[saltHex]; ok {
		return enc, nil
	}

	// Need passphrase to derive key for different salt
	if f.passphraseGetter == nil {
		return nil, fmt.Errorf("cannot read file with different salt: no passphrase getter provided")
	}

	passphrase, zeroBytes, err := f.passphraseGetter()
	if err != nil {
		return nil, fmt.Errorf("failed to get passphrase: %w", err)
	}
	defer zeroBytes()

	// Derive key
	key := encryption.DeriveKeyPBKDF2(passphrase, salt, int(iteration), encryption.KeyLen)

	// Create encryptor
	enc, err := encryption.NewAES256GCMEncryptor(key)
	if err != nil {
		utils.ZeroBytes(key)
		return nil, fmt.Errorf("failed to create encryptor: %w", err)
	}

	// Cache it
	f.encryptorCache[saltHex] = enc

	return enc, nil
}

// writeHeader writes the encryption header to a file.
// Header format: [8-byte magic][16-byte salt][8-byte iteration]
func (f *Factory) writeHeader(fd *os.File, salt []byte, iteration uint64) error {
	if fd == nil {
		return fmt.Errorf("file descriptor is nil")
	}

	// 1. Magic header
	if n, err := fd.Write(encryption.FileMagic); err != nil {
		return err
	} else if n != len(encryption.FileMagic) {
		return fmt.Errorf("failed to write magic header: wrote %d of %d bytes", n, len(encryption.FileMagic))
	}

	// 2. 128-bits (16 bytes) salt
	if len(salt) != encryption.SaltLen {
		return fmt.Errorf("invalid salt length: %d != %d", len(salt), encryption.SaltLen)
	}
	if n, err := fd.Write(salt); err != nil {
		return err
	} else if n != len(salt) {
		return fmt.Errorf("failed to write salt: wrote %d of %d bytes", n, len(salt))
	}

	// 3. uint64 PBKDF2 iteration count (big endian)
	var iterBuf [encryption.IterLen]byte
	binary.BigEndian.PutUint64(iterBuf[:], iteration)
	if n, err := fd.Write(iterBuf[:]); err != nil {
		return err
	} else if n != len(iterBuf) {
		return fmt.Errorf("failed to write iteration count: wrote %d of %d bytes", n, len(iterBuf))
	}

	return nil
}

// readHeader reads and validates the encryption header, returning salt and iteration.
// Positions file pointer after the header for subsequent reads.
func (f *Factory) readHeader(fd *os.File) (salt []byte, iteration uint64, err error) {
	if fd == nil {
		return nil, 0, fmt.Errorf("file descriptor is nil")
	}

	headerLen := len(encryption.FileMagic) + encryption.SaltLen + encryption.IterLen

	stat, err := fd.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat error: %w", err)
	}
	if stat.Size() < int64(headerLen) {
		return nil, 0, fmt.Errorf("file too small: %d < %d", stat.Size(), headerLen)
	}

	if _, err = fd.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to start of file: %w", err)
	}

	buf := make([]byte, headerLen)
	if n, err := io.ReadFull(fd, buf); err != nil {
		return nil, 0, fmt.Errorf("failed to read header: %w", err)
	} else if n < headerLen {
		return nil, 0, fmt.Errorf("failed to read header: read %d of %d bytes", n, headerLen)
	}

	// Check magic
	if !bytes.Equal(buf[:len(encryption.FileMagic)], encryption.FileMagic) {
		return nil, 0, encryption.ErrorEncryptionFormatUnrecog
	}

	// Extract salt
	saltStart := len(encryption.FileMagic)
	salt = make([]byte, encryption.SaltLen)
	copy(salt, buf[saltStart:saltStart+encryption.SaltLen])

	// Extract iteration
	iterStart := saltStart + encryption.SaltLen
	iteration = binary.BigEndian.Uint64(buf[iterStart:])
	if iteration == 0 {
		return nil, 0, fmt.Errorf("invalid iteration count 0")
	}

	return salt, iteration, nil
}
