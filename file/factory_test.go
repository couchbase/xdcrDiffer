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
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/couchbase/xdcrDiffer/encryption"
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

func TestFactory_Init_Idempotent(t *testing.T) {
	passphrase := []byte("once only")
	factory := NewFactory(true, encryption.PBKDF2, nil)

	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Second Init should fail
	err = factory.InitEncryption(false, passphrase)
	assert.Error(t, err)
}

func TestFactory_Init_DisabledEncryption(t *testing.T) {
	factory := NewFactory(false, encryption.PBKDF2, nil)

	// Init should succeed even without passphrase when encryption is disabled
	err := factory.InitEncryption(false, nil)
	assert.NoError(t, err)

	// Should not be initialized (encryption disabled)
	assert.False(t, factory.IsInitialized())
}

func TestFactory_GetSuffix(t *testing.T) {
	tests := []struct {
		name              string
		encryptionEnabled bool
		want              string
	}{
		{
			name:              "enabled returns suffix",
			encryptionEnabled: true,
			want:              encryption.EncSuffix,
		},
		{
			name:              "disabled returns empty",
			encryptionEnabled: false,
			want:              "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory(tt.encryptionEnabled, encryption.PBKDF2, nil)
			got := factory.GetSuffix()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFactory_IsEncrypted(t *testing.T) {
	enabledFactory := NewFactory(true, encryption.PBKDF2, nil)
	assert.True(t, enabledFactory.IsEncrypted())

	disabledFactory := NewFactory(false, encryption.PBKDF2, nil)
	assert.False(t, disabledFactory.IsEncrypted())
}

func TestFactory_OpenFile_PlainFile(t *testing.T) {
	factory := NewFactory(false, encryption.PBKDF2, nil)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "plain_test_*")
	assert.NoError(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFileName)

	// Open for writing
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	assert.NotNil(t, f)

	// Should be a PlainFile
	_, ok := f.(*PlainFile)
	assert.True(t, ok)

	// Write data
	testData := []byte("plain text content")
	n, err := f.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	f.Close()

	// Read back
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	readData, err := f2.ReadAll()
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(testData, readData))
	f2.Close()
}

func TestFactory_OpenFile_EncryptedFile_NotInitialized(t *testing.T) {
	factory := NewFactory(true, encryption.PBKDF2, nil)

	// Should fail without Init
	_, err := factory.OpenFile("/tmp/test.enc", os.O_RDWR|os.O_CREATE, 0644, WriteMode)
	assert.Error(t, err)
}

func TestFactory_WriteHeader_And_ReadHeader(t *testing.T) {
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory := NewFactory(true, encryption.PBKDF2, nil)

	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "header_test_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write header using internal method (via OpenFile)
	err = factory.writeHeader(tmpFile)
	assert.NoError(t, err)

	// Read and verify header contents
	tmpFile.Seek(0, 0)
	data, err := os.ReadFile(tmpFile.Name())
	assert.NoError(t, err)

	// Header should be exactly 80 bytes
	assert.Equal(t, 80, len(data), "Header should be 80 bytes")

	// Check magic (21 bytes)
	assert.True(t, bytes.HasPrefix(data, encryption.FileMagic))

	// Check version (byte 21)
	assert.Equal(t, byte(encryption.VersionKeyDerivation), data[21], "Version should be VersionKeyDerivation")

	// Check compression (byte 22) - should be no compression
	assert.Equal(t, byte(encryption.Nocompression), data[22], "Compression should be Nocompression")

	// Check KDF byte (byte 23) - PBKDF2 (0) in upper nibble, exponent in lower nibble
	kdfByte := data[23]
	assert.Equal(t, encryption.PBKDF2, computeKDFFromHeaderValue(kdfByte), "KDF should be PBKDF2")
	iterFromKDF := computeIterationFromHeaderValue(kdfByte)
	assert.Greater(t, iterFromKDF, uint64(0), "Iteration count should be > 0")
	assert.LessOrEqual(t, iterFromKDF, uint64(encryption.MaxIterations), "Iteration should be <= MaxIterations")

	// Check unused bytes (bytes 24-26) - should be 0
	assert.Equal(t, []byte{0, 0, 0}, data[24:27], "Unused bytes should be 0")

	// Check ID length (byte 27)
	assert.Equal(t, byte(36), data[27], "ID length should be 36")

	// Check ID bytes (bytes 28-63) - should be "password" padded
	idBytes := data[28:64]
	assert.True(t, bytes.HasPrefix(idBytes, []byte(encryption.KeyID)), "ID should start with KeyID")

	// Check salt (bytes 64-79)
	saltFromFile := data[64:80]
	assert.True(t, bytes.Equal(factory.sessionConfig.Salt, saltFromFile), "Salt should match session salt")

	// Read header via internal method
	tmpFile.Seek(0, 0)
	salt, iteration, err := factory.readHeader(tmpFile)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(factory.sessionConfig.Salt, salt))
	// Note: iteration is derived from exponent, may not exactly match sessionConfig.Iteration
	assert.Greater(t, iteration, uint64(0), "Iteration should be > 0")
}

func TestFactory_OpenFile_EncryptedFile_Success(t *testing.T) {
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory := NewFactory(true, encryption.PBKDF2, nil)

	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Create temp file with encryption suffix
	pattern := "openfile_*" + factory.GetSuffix()
	tmpFile, err := os.CreateTemp("", pattern)
	assert.NoError(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFileName)

	// Open for writing
	f, err := factory.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	assert.NotNil(t, f)

	// Should be an EncryptedFile
	_, ok := f.(*EncryptedFile)
	assert.True(t, ok)

	// Write data
	plaintext := []byte("secret content that will be encrypted")
	n, err := f.Write(plaintext)
	assert.NoError(t, err)
	assert.Equal(t, len(plaintext), n)
	f.Close()

	// File size should be larger than plaintext (header + nonce + tag)
	info, _ := os.Stat(tmpFileName)
	assert.Greater(t, info.Size(), int64(len(plaintext)))

	// Reopen and read
	f2, err := factory.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	readData, err := f2.ReadAll()
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext, readData))
	f2.Close()
}

func TestFactory_OpenFile_ExistingFile_DifferentSalt(t *testing.T) {
	// Create first factory and write encrypted file
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory1 := NewFactory(true, encryption.PBKDF2, nil)
	err := factory1.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	tmpFile, err := os.CreateTemp("", "diffsalt_*.enc")
	assert.NoError(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFileName)

	// Write with factory1
	f, err := factory1.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644, WriteMode)
	assert.NoError(t, err)
	plaintext := []byte("test data for different salt")
	f.Write(plaintext)
	f.Close()

	// Create second factory with same passphrase but different salt
	passphraseGetter := func() ([]byte, func(), error) {
		return passphrase, func() {}, nil
	}
	factory2 := NewFactory(true, encryption.PBKDF2, passphraseGetter)
	err = factory2.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Open file created by factory1 - should derive key from header salt
	f2, err := factory2.OpenFile(tmpFileName, os.O_RDONLY, 0, ReadMode)
	assert.NoError(t, err)

	readData, err := f2.ReadAll()
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext, readData))
	f2.Close()
}

// Tests for computeIterationFromHeaderValue
func TestComputeIterationFromHeaderValue(t *testing.T) {
	tests := []struct {
		name     string
		kdfByte  uint8
		expected uint64
	}{
		{
			name:     "exponent 0 gives 1024 iterations",
			kdfByte:  0x00, // exponent=0, KDF=0 (PBKDF2)
			expected: 1024,
		},
		{
			name:     "exponent 1 gives 2048 iterations",
			kdfByte:  0x10, // exponent=1 in high nibble, KDF=0 in low nibble
			expected: 2048,
		},
		{
			name:     "exponent 10 gives 1048576 iterations",
			kdfByte:  0xA0, // exponent=10 in high nibble, KDF=0 in low nibble
			expected: 1024 * (1 << 10),
		},
		{
			name:     "exponent 15 gives max iterations",
			kdfByte:  0xF0,             // exponent=15 in high nibble, KDF=0 in low nibble
			expected: 1024 * (1 << 15), // 33,554,432
		},
		{
			name:     "lower nibble is ignored for iteration",
			kdfByte:  0x5F, // exponent=5 in high nibble, KDF=15 (ignored) in low nibble
			expected: 1024 * (1 << 5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeIterationFromHeaderValue(tt.kdfByte)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Tests for computeKDFFromHeaderValue
func TestComputeKDFFromHeaderValue(t *testing.T) {
	tests := []struct {
		name     string
		kdfByte  byte
		expected encryption.KDF
	}{
		{
			name:     "PBKDF2 (0) in lower nibble",
			kdfByte:  0x00, // exponent=0, KDF=0
			expected: encryption.PBKDF2,
		},
		{
			name:     "PBKDF2 with non-zero exponent",
			kdfByte:  0xF0, // exponent=15 in high nibble, KDF=0 in low nibble
			expected: encryption.PBKDF2,
		},
		{
			name:     "KDF=1 in lower nibble",
			kdfByte:  0x01, // exponent=0, KDF=1
			expected: encryption.KDF(1),
		},
		{
			name:     "KDF=15 in lower nibble",
			kdfByte:  0x0F, // exponent=0, KDF=15
			expected: encryption.KDF(15),
		},
		{
			name:     "upper nibble is ignored for KDF",
			kdfByte:  0xF2, // exponent=15 (ignored), KDF=2
			expected: encryption.KDF(2),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeKDFFromHeaderValue(tt.kdfByte)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Tests for computeKeyDerivationForHeader
func TestComputeKeyDerivationForHeader(t *testing.T) {
	tests := []struct {
		name           string
		iterationCount uint64
		kdf            encryption.KDF
		// We check roundtrip: the KDF byte should decode to same KDF
		// and an iteration count >= 1024
	}{
		{
			name:           "PBKDF2 with 1024 iterations",
			iterationCount: 1024,
			kdf:            encryption.PBKDF2,
		},
		{
			name:           "PBKDF2 with 100000 iterations",
			iterationCount: 100000,
			kdf:            encryption.PBKDF2,
		},
		{
			name:           "PBKDF2 with max iterations",
			iterationCount: encryption.MaxIterations,
			kdf:            encryption.PBKDF2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kdfByte := computeKeyDerivationForHeader(tt.iterationCount, tt.kdf)

			// Verify KDF can be extracted
			extractedKDF := computeKDFFromHeaderValue(kdfByte)
			assert.Equal(t, tt.kdf, extractedKDF, "KDF should round-trip correctly")

			// Verify iteration can be extracted (may be approximated)
			extractedIter := computeIterationFromHeaderValue(kdfByte)
			assert.Greater(t, extractedIter, uint64(0), "Iteration should be > 0")
			assert.LessOrEqual(t, extractedIter, uint64(encryption.MaxIterations), "Iteration should be <= MaxIterations")
		})
	}
}

// Tests for quantizeIterationCount
func TestQuantizeIterationCount(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{
			name:     "minimum_iterations",
			input:    500, // below minimum
			expected: 1024,
		},
		{
			name:     "exact_1024",
			input:    1024,
			expected: 1024,
		},
		{
			name:     "exact_2048",
			input:    2048,
			expected: 2048,
		},
		{
			name:     "between_powers_of_2_rounds_down",
			input:    100000, // between 65536 (2^16) and 131072 (2^17)
			expected: 65536,  // 1024 * 2^6 = 65536
		},
		{
			name:     "large_value_rounds_down",
			input:    500000,
			expected: 262144, // 1024 * 2^8 = 262144 (log2(500000/1024) ≈ 8.93 → floor = 8)
		},
		{
			name:     "max_exponent_15",
			input:    50000000, // very large
			expected: 33554432, // 1024 * 2^15 (max)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quantizeIterationCount(tt.input)
			assert.Equal(t, tt.expected, result)

			// Verify round-trip: quantized value should encode/decode exactly
			kdfByte := computeKeyDerivationForHeader(uint64(result), encryption.PBKDF2)
			decoded := computeIterationFromHeaderValue(kdfByte)
			assert.Equal(t, uint64(result), decoded, "quantized value should round-trip exactly")
		})
	}
}

// Test header format validation errors
func TestFactory_ReadHeader_ValidationErrors(t *testing.T) {
	factory := NewFactory(true, encryption.PBKDF2, nil)
	err := factory.InitEncryption(false, []byte("test-passphrase"))
	assert.NoError(t, err)

	t.Run("file too small", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "small_header_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write less than 80 bytes
		tmpFile.Write([]byte("too small"))
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file too small")
		tmpFile.Close()
	})

	t.Run("invalid magic", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_magic_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write 80 bytes with wrong magic
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], []byte("Not Couchbase Magic!X"))
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.ErrorIs(t, err, encryption.ErrorEncryptionFormatUnrecog)
		tmpFile.Close()
	})

	t.Run("unsupported version", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_version_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with old version
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionNoKeyDerivation) // Version 0
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.ErrorIs(t, err, encryption.ErrorEncryptionFormatUnsupported)
		tmpFile.Close()
	})

	t.Run("unsupported compression", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_compression_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with compression enabled
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionKeyDerivation)
		header[22] = byte(encryption.Snappy) // Compression not supported
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.ErrorIs(t, err, encryption.ErrorCompressionUnsupported)
		tmpFile.Close()
	})

	t.Run("unsupported KDF", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_kdf_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with unsupported KDF
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionKeyDerivation)
		header[22] = byte(encryption.Nocompression)
		header[23] = 0x51 // exponent=5 in high nibble, KDF=1 (not PBKDF2) in low nibble
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.ErrorIs(t, err, encryption.ErrorKDFUnsupported)
		tmpFile.Close()
	})

	t.Run("invalid ID length", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_idlen_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with wrong ID length
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionKeyDerivation)
		header[22] = byte(encryption.Nocompression)
		header[23] = 0x50 // exponent=5 in high nibble, KDF=0 (PBKDF2) in low nibble
		// Unused bytes 24-26 are 0
		header[27] = 32 // Wrong ID length (should be 36)
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid key ID length")
		tmpFile.Close()
	})

	t.Run("unrecognized key ID", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_keyid_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with wrong key ID
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionKeyDerivation)
		header[22] = byte(encryption.Nocompression)
		header[23] = 0x50 // exponent=5 in high nibble, KDF=0 (PBKDF2) in low nibble
		// Unused bytes 24-26 are 0
		header[27] = 36                                                 // Correct ID length
		copy(header[28:64], []byte("not-the-correct-password-key-id!")) // Wrong key ID
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized key ID")
		tmpFile.Close()
	})

	t.Run("invalid key ID padding", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bad_padding_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write header with correct key ID but non-zero padding
		header := make([]byte, encryption.HeaderLength)
		copy(header[0:21], encryption.FileMagic)
		header[21] = byte(encryption.VersionKeyDerivation)
		header[22] = byte(encryption.Nocompression)
		header[23] = 0x50 // exponent=5 in high nibble, KDF=0 (PBKDF2) in low nibble
		// Unused bytes 24-26 are 0
		header[27] = 36 // Correct ID length
		// Copy correct key ID "password" (8 bytes)
		copy(header[28:36], []byte(encryption.KeyID))
		// Fill remaining 28 bytes with non-zero values (invalid padding)
		for i := 36; i < 64; i++ {
			header[i] = 0xFF
		}
		// Salt at bytes 64-79
		copy(header[64:80], []byte("0123456789abcdef"))
		tmpFile.Write(header)
		tmpFile.Seek(0, 0)

		_, _, err = factory.readHeader(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid key ID padding")
		tmpFile.Close()
	})
}

// Test writeHeader edge cases
func TestFactory_WriteHeader_EdgeCases(t *testing.T) {
	factory := NewFactory(true, encryption.PBKDF2, nil)
	err := factory.InitEncryption(false, []byte("test-passphrase"))
	assert.NoError(t, err)

	t.Run("nil file descriptor", func(t *testing.T) {
		err := factory.writeHeader(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file descriptor is nil")
	})

	t.Run("file pointer not at start", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "not_at_start_*.enc")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		// Write some data first
		tmpFile.Write([]byte("some data"))
		// Don't seek back to start

		err = factory.writeHeader(tmpFile)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file descriptor not at start")
	})
}

// Test that header roundtrip works correctly
func TestFactory_HeaderRoundtrip(t *testing.T) {
	passphrase := []byte("roundtrip-test-passphrase")
	factory := NewFactory(true, encryption.PBKDF2, nil)
	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	tmpFile, err := os.CreateTemp("", "roundtrip_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write header
	err = factory.writeHeader(tmpFile)
	assert.NoError(t, err)

	// Read back
	tmpFile.Seek(0, 0)
	salt, iteration, err := factory.readHeader(tmpFile)
	assert.NoError(t, err)

	// Salt should match exactly
	assert.True(t, bytes.Equal(factory.sessionConfig.Salt, salt), "Salt should match")

	// Iteration should be derived from the exponent encoding
	// It may not match exactly due to the exponent encoding, but should be > 0
	assert.Greater(t, iteration, uint64(0), "Iteration should be > 0")
	assert.LessOrEqual(t, iteration, uint64(encryption.MaxIterations), "Iteration should be <= MaxIterations")
}

// Test decrypt mode initialization
func TestFactory_InitEncryption_DecryptMode(t *testing.T) {
	factory := NewFactory(true, encryption.PBKDF2, nil)

	// In decrypt mode, passphrase can be nil
	err := factory.InitEncryption(true, nil)
	assert.NoError(t, err)
	assert.True(t, factory.IsInitialized())

	// Second init should fail
	err = factory.InitEncryption(true, nil)
	assert.Error(t, err)
}

// Test that sessionConfig is properly set with KDF
func TestFactory_SessionConfig(t *testing.T) {
	factory := NewFactory(true, encryption.PBKDF2, nil)
	assert.NotNil(t, factory.sessionConfig)
	assert.Equal(t, encryption.PBKDF2, factory.sessionConfig.KeyDerivationAlgo)

	err := factory.InitEncryption(false, []byte("test-passphrase"))
	assert.NoError(t, err)

	// After init, salt and iteration should be set
	assert.NotNil(t, factory.sessionConfig.Salt)
	assert.Equal(t, encryption.SaltLen, len(factory.sessionConfig.Salt))
	assert.Greater(t, factory.sessionConfig.Iteration, uint64(0))
}
