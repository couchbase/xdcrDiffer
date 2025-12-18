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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/couchbase/xdcrDiffer/encryption"
	"github.com/couchbase/xdcrDiffer/utils"
)

// Factory creates File instances (either PlainFile or EncryptedFile).
type Factory struct {
	// encryptionEnabled denotes if encryption is enabled.
	encryptionEnabled bool
	// sessionConfig denotes the key-derivation configuration parameters for this session.
	sessionConfig *encryption.Config
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
func NewFactory(encryptionEnabled bool, kdf encryption.KDF, passphraseGetter encryption.PassphraseGetter) *Factory {
	f := &Factory{
		encryptionEnabled: encryptionEnabled,
		passphraseGetter:  passphraseGetter,
		sessionConfig:     &encryption.Config{KeyDerivationAlgo: kdf},
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
	iteration := encryption.CalibrateIterations(passphrase, []byte(encryption.SaltPrefix+string(salt)), encryption.KeyLen, encryption.CalibrationBudget, encryption.TargetDerivationTime)
	if iteration == 0 {
		return fmt.Errorf("calibration returned 0 iterations")
	}

	// Quantize iteration count to a value that can be exactly represented in the header.
	// The header format stores iterations as 1024 * 2^exponent, so we must use a quantized value
	// to ensure the same iteration count is used during both encryption and decryption.
	iteration = quantizeIterationCount(iteration)

	// Derive key from passphrase
	key := encryption.DeriveKeyPBKDF2(passphrase, []byte(encryption.SaltPrefix+string(salt)), iteration, encryption.KeyLen)

	// Create session encryptor
	encryptor, err := encryption.NewAES256GCMEncryptor(key)
	if err != nil {
		// Zero the key on failure
		utils.ZeroBytes(key)
		return fmt.Errorf("failed to create encryptor: %w", err)
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.sessionConfig.Salt = salt
	f.sessionConfig.Iteration = uint64(iteration)
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

// openEncryptedFile opens an encrypted file, handling headers and encryptor setup.
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

	return NewEncryptedFile(fd, path, encryptor, mode), nil
}

// setupNewFile writes the session header and returns the session encryptor.
func (f *Factory) setupNewFile(fd *os.File) (*encryption.AES256GCMEncryptor, error) {
	// Seek to start of file before writing header
	if pos, err := fd.Seek(0, io.SeekStart); err != nil || pos != 0 {
		return nil, fmt.Errorf("failed to seek to start: pos=%d err=%v", pos, err)
	}

	// Write header with session salt and iteration
	if err := f.writeHeader(fd); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	f.mtx.RLock()
	defer f.mtx.RUnlock()

	encryptor, exists := f.encryptorCache[hex.EncodeToString(f.sessionConfig.Salt)]
	if !exists {
		return nil, errors.New("encryptor not found in cache")
	}
	return encryptor, nil
}

// setupExistingFile reads the header and returns the appropriate encryptor.
// If the salt matches session salt, returns session encryptor.
// Otherwise, derives a new key using passphraseGetter.
func (f *Factory) setupExistingFile(fd *os.File) (*encryption.AES256GCMEncryptor, error) {
	// Seek to start of file before reading header
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}
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
	key := encryption.DeriveKeyPBKDF2(passphrase, []byte(encryption.SaltPrefix+string(salt)), int(iteration), encryption.KeyLen)

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
// Header format: refer - https://github.com/couchbase/platform/blob/master/cbcrypto/EncryptedFileFormat.md
// | offset | length | description                    |
// +--------+--------+--------------------------------+
// | 0      | 21     | magic: \0Couchbase Encrypted\0 |
// | 21     | 1      | version                        |
// | 22     | 1      | compression                    |
// | 23     | 1      | key derivation                 |
// | 24     | 3      | unused (should be set to 0)    |
// | 27     | 1      | id len                         |
// | 28     | 36     | id bytes                       |
// | 64     | 16     | salt (uuid)                    |
// +--------+--------+--------------------------------+
// Total of 80 bytes

// computeIterationFromHeaderValue computes the iteration count from the header byte.
// The most significant 4 bits encode the iteration exponent.
func computeIterationFromHeaderValue(kdf uint8) uint64 {
	exponent := kdf >> 4
	return 1024 * (1 << exponent)
}

// computeKDFFromHeaderValue computes the KDF from the header byte.
// The least significant 4 bits determine the key derivation method.
func computeKDFFromHeaderValue(kdf byte) encryption.KDF {
	return encryption.KDF(kdf & 0x0F)
}

// quantizeIterationCount rounds down the iteration count to the nearest representable value.
// The header format can only store iterations as 1024 * 2^exponent where exponent is 0-15.
func quantizeIterationCount(iteration int) int {
	if iteration < 1024 {
		return 1024 // minimum
	}
	exponent := int(math.Log2(float64(iteration) / 1024))
	if exponent > 15 {
		// max exponent that fits in 4 bits
		exponent = 15
	}
	return 1024 * (1 << exponent)
}

// computeKeyDerivationForHeader computes the header byte for the given iteration count and KDF.
// iteration = 1024 * 2^exponent, so exponent = log2(iteration / 1024)
// The least significant 4 bits encode the KDF method, the most significant 4 bits encode the iteration exponent.
func computeKeyDerivationForHeader(iterationCount uint64, kdf encryption.KDF) byte {
	exponent := uint8(math.Log2(float64(iterationCount) / 1024))
	kdfByte := uint8(kdf)
	return (exponent << 4) | (kdfByte & 0x0F)
}

// writeHeader writes the encryption header to the file descriptor.
func (f *Factory) writeHeader(fd *os.File) error {
	if fd == nil {
		return fmt.Errorf("file descriptor is nil")
	}

	// Ensure file pointer is at start
	if pos, err := fd.Seek(0, io.SeekCurrent); err != nil || pos != 0 {
		return fmt.Errorf("file descriptor not at start of file: pos=%d err=%v", pos, err)
	}

	header := make([]byte, encryption.HeaderLength)
	pos := 0

	// 1. Magic header
	copy(header[pos:encryption.MagicLength], encryption.FileMagic)
	pos += encryption.MagicLength
	// 2. Write the version
	header[pos] = byte(encryption.VersionKeyDerivation)
	pos++
	// 3. Write the compression (no compression)
	header[pos] = byte(encryption.Nocompression)
	pos++
	// 4. Write the KDF (PBKDF2)
	header[pos] = computeKeyDerivationForHeader(f.sessionConfig.Iteration, encryption.PBKDF2)
	pos++
	// 5. Unused (3 bytes)
	pos += 3
	// 6. ID length (1 byte, 36 bytes for UUID)
	header[pos] = byte(36)
	pos++
	// 7. ID bytes (36 bytes)
	copy(header[pos:pos+36], []byte(encryption.KeyID))
	pos += 36
	// 8. Salt (16 bytes)
	copy(header[pos:], f.sessionConfig.Salt)
	pos += 16

	if pos != 80 {
		return fmt.Errorf("error constructing header: expected the pos to be at 80 but is at %v", pos)
	}

	// Write the header to the file
	n, err := fd.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if n != 80 {
		return fmt.Errorf("failed to write complete header: wrote %d of 80 bytes", n)
	}

	return nil
}

// readHeader reads and validates the encryption header, returning salt and iteration.
// Positions file pointer after the header for subsequent reads.
func (f *Factory) readHeader(fd *os.File) (salt []byte, iteration uint64, err error) {
	if fd == nil {
		return nil, 0, fmt.Errorf("file descriptor is nil")
	}

	headerLen := 80

	stat, err := fd.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat error: %w", err)
	}
	if stat.Size() < int64(headerLen) {
		return nil, 0, fmt.Errorf("file too small: %d < %d", stat.Size(), headerLen)
	}

	// Ensure file pointer is at start
	if pos, err := fd.Seek(0, io.SeekCurrent); err != nil || pos != 0 {
		return nil, 0, fmt.Errorf("file descriptor not at start of file: pos=%d err=%v", pos, err)
	}

	// read the header
	header := make([]byte, headerLen)
	pos := 0
	n, err := io.ReadFull(fd, header)
	if n != 80 {
		return nil, 0, fmt.Errorf("failed to read full header: read %d of 80 bytes, err=%v", n, err)
	}

	// 1. Check magic
	if !bytes.Equal(header[pos:pos+encryption.MagicLength], encryption.FileMagic) {
		return nil, 0, encryption.ErrorEncryptionFormatUnrecog
	}
	pos += encryption.MagicLength
	// 2. Read the version and ensure it supports PBKDF
	version := encryption.Version(header[pos])
	pos++
	if version < encryption.VersionKeyDerivation {
		return nil, 0, encryption.ErrorEncryptionFormatUnsupported
	}
	// 3. Read the compression algo used (if any)
	compression := encryption.Compression(header[pos])
	pos++
	if compression != encryption.Nocompression {
		// Differ does not support compression currently
		return nil, 0, encryption.ErrorCompressionUnsupported
	}
	// 4. Read the KDF and iteration exponent
	kdfByte := header[pos]
	pos++
	kdf := computeKDFFromHeaderValue(kdfByte)
	if kdf != encryption.PBKDF2 {
		// Differ only supports PBKDF2 currently
		return nil, 0, encryption.ErrorKDFUnsupported
	}
	iteration = computeIterationFromHeaderValue(kdfByte)
	if iteration == 0 || iteration > encryption.MaxIterations {
		return nil, 0, fmt.Errorf("invalid iteration count derived from header: %d", iteration)
	}
	// 5. Unused (3 bytes)
	pos += 3
	// 6. ID length (1 byte)
	idLen := int(header[pos])
	pos++
	if idLen != 36 {
		return nil, 0, fmt.Errorf("invalid key ID length: %d", idLen)
	}
	// 7. ID bytes (36 bytes)
	idBytes := header[pos : pos+36]
	keyIDBytes := []byte(encryption.KeyID)
	keyIDLen := len(keyIDBytes)
	// Check that ID starts with KeyID
	if !bytes.Equal(idBytes[:keyIDLen], keyIDBytes) {
		return nil, 0, fmt.Errorf("unrecognized key ID: %s", string(idBytes[:keyIDLen]))
	}
	// Check that remaining bytes are zero-padded
	paddingLen := 36 - keyIDLen
	zeroPadding := make([]byte, paddingLen)
	if !bytes.Equal(idBytes[keyIDLen:], zeroPadding) {
		return nil, 0, fmt.Errorf("invalid key ID padding: expected zero bytes")
	}
	pos += 36
	// 8. Salt (16 bytes)
	salt = make([]byte, encryption.SaltLen)
	copy(salt, header[pos:pos+encryption.SaltLen])
	pos += encryption.SaltLen

	if pos != headerLen {
		return nil, 0, fmt.Errorf("error parsing header: expected the pos to be at %d but is at %d", headerLen, pos)
	}

	return salt, iteration, nil
}
