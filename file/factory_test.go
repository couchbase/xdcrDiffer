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
	"encoding/binary"
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
	factory := NewFactory(true, nil)

	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Second Init should fail
	err = factory.InitEncryption(false, passphrase)
	assert.Error(t, err)
}

func TestFactory_Init_DisabledEncryption(t *testing.T) {
	factory := NewFactory(false, nil)

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
			factory := NewFactory(tt.encryptionEnabled, nil)
			got := factory.GetSuffix()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFactory_IsEncrypted(t *testing.T) {
	enabledFactory := NewFactory(true, nil)
	assert.True(t, enabledFactory.IsEncrypted())

	disabledFactory := NewFactory(false, nil)
	assert.False(t, disabledFactory.IsEncrypted())
}

func TestFactory_OpenFile_PlainFile(t *testing.T) {
	factory := NewFactory(false, nil)

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
	factory := NewFactory(true, nil)

	// Should fail without Init
	_, err := factory.OpenFile("/tmp/test.enc", os.O_RDWR|os.O_CREATE, 0644, WriteMode)
	assert.Error(t, err)
}

func TestFactory_WriteHeader_And_ReadHeader(t *testing.T) {
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory := NewFactory(true, nil)

	err := factory.InitEncryption(false, passphrase)
	assert.NoError(t, err)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "header_test_*.enc")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write header using internal method (via OpenFile)
	err = factory.writeHeader(tmpFile, factory.sessionSalt, factory.sessionIteration)
	assert.NoError(t, err)

	// Read and verify header contents
	tmpFile.Seek(0, 0)
	data, err := os.ReadFile(tmpFile.Name())
	assert.NoError(t, err)

	// Check magic
	assert.True(t, bytes.HasPrefix(data, encryption.FileMagic))

	// Check salt
	saltStart := len(encryption.FileMagic)
	saltFromFile := data[saltStart : saltStart+encryption.SaltLen]
	assert.True(t, bytes.Equal(factory.sessionSalt, saltFromFile))

	// Check iteration
	iterStart := saltStart + encryption.SaltLen
	iterFromFile := binary.BigEndian.Uint64(data[iterStart:])
	assert.Equal(t, factory.sessionIteration, iterFromFile)

	// Read header via internal method
	tmpFile.Seek(0, 0)
	salt, iteration, err := factory.readHeader(tmpFile)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(factory.sessionSalt, salt))
	assert.Equal(t, factory.sessionIteration, iteration)
}

func TestFactory_OpenFile_EncryptedFile_Success(t *testing.T) {
	passphrase := []byte(hex.EncodeToString(randomBytes(t, 32)))
	factory := NewFactory(true, nil)

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
	factory1 := NewFactory(true, nil)
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
	factory2 := NewFactory(true, passphraseGetter)
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
